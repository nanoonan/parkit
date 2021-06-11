# pylint: disable = attribute-defined-outside-init, protected-access
import datetime
import functools
import importlib
import importlib.abc
import importlib.util
import inspect
import logging
import os
import pickle
import time
import types
import typing
import uuid

from typing import (
    Any, Callable, Iterator, Optional, Tuple, Union
)

import cloudpickle
import psutil

import parkit.constants as constants

from parkit.adapters.array import Array
from parkit.adapters.dict import Dict
from parkit.adapters.object import Object
from parkit.adapters.observer import ModuleObserver
from parkit.adapters.queue import Queue
from parkit.cluster import terminate_node
from parkit.storage.context import transaction_context
from parkit.storage.entity import (
    Entity,
    EntityWrapper
)
from parkit.utility import (
    create_string_digest,
    envexists,
    getenv,
    resolve_path,
    setenv
)

logger = logging.getLogger(__name__)

module_observer = ModuleObserver()

class AsyncTraces(EntityWrapper):

    def __init__(self, traces: Array):
        super().__init__()
        self._traces = traces

    @property
    def entity(self) -> Entity:
        return self._traces

    class AsyncTrace(EntityWrapper):

        def __init__(
            self,
            traces: Array,
            trace_index: int
        ):
            super().__init__()
            self._traces = traces
            self._trace_index = trace_index

        @property
        def entity(self) -> Entity:
            return self._traces

        @property
        def record(self) -> typing.Dict[str, Any]:
            record = self._traces[self._trace_index]
            record['status'] = self._get_status()
            return record

        @property
        def status(self) -> str:
            return self._get_status()

        def _get_status(self) -> str:
            record = self._traces[self._trace_index]
            if record['status'] == 'running':
                running = False
                try:
                    if psutil.pid_exists(record['pid']):
                        proc = psutil.Process(record['pid'])
                        env = proc.environ()
                        if constants.NODE_UID_ENVNAME in env:
                            if env[constants.NODE_UID_ENVNAME] == record['node_uid']:
                                running = proc.is_running()
                except psutil.NoSuchProcess:
                    pass
                return 'crashed' if not running else record['status']
            return record['status']

        @property
        def result(self) -> Optional[Any]:
            return self._traces[self._trace_index]['result']

        @property
        def error(self) -> Optional[Any]:
            return self._traces[self._trace_index]['error']

        @property
        def index(self) -> int:
            return self._traces[self._trace_index]['index']

        @property
        def pid(self) -> Optional[int]:
            return self._traces[self._trace_index]['pid']

        @property
        def start(self) -> datetime.datetime:
            start_timestamp = self._traces[self._trace_index]['start_timestamp']
            return datetime.datetime.fromtimestamp(start_timestamp / 1e9)

        @property
        def end(self) -> Optional[datetime.datetime]:
            end_timestamp = self._traces[self._trace_index]['end_timestamp']
            if end_timestamp is not None:
                return datetime.datetime.fromtimestamp(end_timestamp / 1e9)
            return None

        @property
        def done(self) -> bool:
            return self.status in ['finished', 'failed', 'crashed']

        def cancel(self):
            record = self.record
            if record['status'] not in ['running', 'submitted']:
                return
            terminate = False
            with transaction_context(self._traces._Entity__env, write = True):
                record = self._traces[self._trace_index]
                if record['status'] == 'submitted':
                    record['status'] = 'cancelled'
                    self._traces[self._trace_index] = record
                elif record['status'] == 'running':
                    terminate = True
            if terminate:
                terminate_node(
                    record['node_uid'],
                    self._traces.site_uuid
                )

        def __str__(self) -> str:
            record = self.record
            return """
AsyncTrace
task: {0}
status: {1}
pid: {2}
start: {3}
end: {4}
result: {5}
error: {6}
            """.format(
                record['path'], record['status'], record['pid'],
                datetime.datetime.fromtimestamp(record['start_timestamp'] / 1e9),
                None if record['end_timestamp'] is None else \
                datetime.datetime.fromtimestamp(record['end_timestamp'] / 1e9),
                record['result'], record['error']
            ).strip()

    class ReversibleGetSlice():

        def __init__(
            self,
            traces: Array,
            source: Any
        ):
            self._traces = traces
            self._source = source

        def __reversed__(self) -> Iterator[Any]:
            for record in self._source.__reversed__():
                yield AsyncTraces.AsyncTrace(self._traces, record['index'])

        def __iter__(self) -> Iterator[Any]:
            for record in self._source.__iter__():
                yield AsyncTraces.AsyncTrace(self._traces, record['index'])

    def __getitem__(
        self,
        key: Union[int, slice],
        /
    ) -> Any:
        if isinstance(key, slice):
            return AsyncTraces.ReversibleGetSlice(self._traces, self._traces[key])
        return AsyncTraces.AsyncTrace(self._traces, key)

    def __iter__(self):
        for record in self._traces:
            yield AsyncTraces.AsyncTrace(self._traces, record['index'])

    def __bool__(self) -> bool:
        return self.count() > 0

    def __len__(self) -> int:
        return len(self._traces)

    def count(self) -> int:
        return len(self._traces)

class Process(Object):

    _target_function: Optional[Callable[..., Any]] = None

    def __init__(
        self,
        path: str,
        /, *,
        target: Optional[Callable[..., Any]] = None,
        create: bool = True,
        bind: bool = True,
        metadata: Optional[typing.Dict[str, Any]] = None,
        site: Optional[str] = None,
        on_init: Optional[Callable[[bool], None]] = None
    ):
        namespace, _ = resolve_path(path)

        if namespace != constants.TASK_NAMESPACE:
            raise ValueError()

        if target:
            module = inspect.getmodule(target)
            if not hasattr(module, '__file__'):
                bytecode = cloudpickle.dumps(target)
                digest = 'bytecode-{0}'.format(
                    create_string_digest(bytecode)
                )
            else:
                assert isinstance(module, types.ModuleType)
                digest = module_observer.get_digest(
                    module.__name__,
                    target.__name__
                )

        def load_target():
            if target:
                self._target_function = target
                if not hasattr(module, '__file__'):
                    if not self.__latest or self.__latest[-1] != digest:
                        if digest not in self.__code.keys():
                            self.__code[digest] = bytecode
                        if not self.__latest or self.__latest[-1] != digest:
                            self.__latest.append(digest)
                else:
                    if not self.__latest or self.__latest[-1] != digest:
                        if digest not in self.__code.keys():
                            self.__code[digest] = (
                                module.__name__,
                                self._target_function.__name__
                            )
                        if not self.__latest or self.__latest[-1] != digest:
                            self.__latest.append(digest)

        def on_init_(create: bool):
            if create:
                self.__traces = Array('/'.join([
                    constants.TASK_NAMESPACE,
                    '__{0}__'.format(str(uuid.uuid4()))
                ]), site = site)
                self.__latest = Array('/'.join([
                    constants.TASK_NAMESPACE,
                    '__{0}__'.format(str(uuid.uuid4()))
                ]), site = site)
                self.__code = Dict('/'.join([
                    constants.TASK_NAMESPACE,
                    '__{0}__'.format(str(uuid.uuid4()))
                ]), site = site)
                load_target()
            else:
                with transaction_context(self._Entity__env, write = True):
                    load_target()
            if on_init:
                on_init(create)

        super().__init__(
            path,
            create = create, bind = bind,
            on_init = on_init_, versioned = False,
            metadata = metadata, site = site
        )

    @property
    def traces(self) -> AsyncTraces:
        return AsyncTraces(self.__traces)

    @functools.lru_cache(None)
    def __bytecode_cache(self, target_digest: str) -> Callable[..., Any]:
        assert target_digest in self.__code
        return cloudpickle.loads(self.__code[target_digest])

    @functools.lru_cache(None)
    def __module_cache(self, target_digest: str) -> Callable[..., Any]:
        assert target_digest in self.__code
        module_name, function_name = self.__code[target_digest]
        spec = importlib.util.find_spec(module_name)
        if spec is None:
            raise ModuleNotFoundError(module_name)
        assert isinstance(spec.loader, importlib.abc.Loader)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        logger.info(
            'reloaded %s.%s on pid %i',
            module_name, function_name, os.getpid()
        )
        if isinstance(getattr(module, function_name), Process):
            return getattr(module, function_name)._target_function
        return getattr(module, function_name)

    def invoke(
        self,
        /, *,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[typing.Dict[str, Any]] = None
    ) -> Any:
        assert self.__latest
        target_digest = self.__latest[-1]
        args = () if args is None else args
        kwargs = {} if kwargs is None else kwargs
        try:
            restore = getenv(constants.SELF_ENVNAME, str) \
            if envexists(constants.SELF_ENVNAME) else None
            setenv(
                constants.SELF_ENVNAME,
                pickle.dumps(self, 0).decode()
            )
            if target_digest.startswith('bytecode'):
                return self.__bytecode_cache(target_digest)(*args, **kwargs)
            return self.__module_cache(target_digest)(*args, **kwargs)
        finally:
            setenv(
                constants.SELF_ENVNAME,
                restore
            )

    def submit(
        self,
        /, *,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[typing.Dict[str, Any]] = None
    ) -> AsyncTraces.AsyncTrace:
        task_queue = Queue(constants.TASK_QUEUE_PATH, site = self.site)
        try:
            setenv(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME, self.site_uuid)
            with transaction_context(self._Entity__env, write = True):
                trace_index = len(self.__traces)
                self.__traces.append(dict(
                    index = trace_index,
                    path = self.path,
                    result = None,
                    error = None,
                    status = 'submitted',
                    start_timestamp = time.time_ns(),
                    end_timestamp = None,
                    pid = None,
                    node_uid = None
                ))
                task_queue.put((self, trace_index, args, kwargs))
                return AsyncTraces.AsyncTrace(self.__traces, trace_index)
        finally:
            setenv(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME, None)

    def drop(self):
        with transaction_context(self._Entity__env, write = True):
            for record in self.__traces[:]:
                if record['status'] == 'running':
                    terminate_node(
                        record['node_uid'],
                        self.site_uuid
                    )
            self.__traces.drop()
            self.__code.drop()
            self.__latest.drop()
            super().drop()
