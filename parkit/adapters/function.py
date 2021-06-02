# pylint: disable = broad-except, non-parent-init-called, super-init-not-called, no-self-use
# pylint: disable = attribute-defined-outside-init
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
from parkit.adapters.observer import ModuleObserver
from parkit.adapters.queue import Queue
from parkit.cluster.manage import terminate_node
from parkit.storage import (
    Entity,
    transaction
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

class AsyncTraces():

    def __init__(self, traces: Array):
        self._traces = traces

    class AsyncTrace():

        def __init__(
            self,
            traces: Array,
            trace_index: int
        ):
            self._traces = traces
            self._trace_index = trace_index

        @property
        def record(self) -> typing.Dict[str, Any]:
            self._update_status()
            return self._traces[self._trace_index]

        @property
        def status(self) -> str:
            self._update_status()
            return self._traces[self._trace_index]['status']

        def _update_status(self):
            record = self._traces[self._trace_index]
            if record['status'] == 'running':
                running = False
                try:
                    if psutil.pid_exists(record['pid']):
                        proc = psutil.Process(record['pid'])
                        cmdline = proc.cmdline()
                        if record['node_uid'] in cmdline:
                            running = True
                except psutil.NoSuchProcess:
                    pass
                if not running:
                    record['status'] = 'crashed'
                    self._traces[self._trace_index] = record

        @property
        def result(self) -> Optional[Any]:
            return self._traces[self._trace_index]['result']

        @property
        def error(self) -> Optional[Any]:
            return self._traces[self._trace_index]['error']

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
            with transaction(constants.TASK_NAMESPACE):
                record = self._traces[self._trace_index]
                if record['status'] == 'submitted':
                    record['status'] = 'cancelled'
                    self._traces[self._trace_index] = record
                elif record['status'] == 'running':
                    terminate = True
            if terminate:
                terminate_node(
                    record['node_uid'],
                    create_string_digest(self._traces.storage_path)
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
            """.format(
                record['path'], record['status'], record['pid'],
                datetime.datetime.fromtimestamp(record['start_timestamp'] / 1e9),
                None if record['end_timestamp'] is None else \
                datetime.datetime.fromtimestamp(record['end_timestamp'] / 1e9)
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

class Function(Dict):

    _target_function: Optional[Callable[..., Any]] = None

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        target: Optional[Callable[..., Any]] = None,
        create: bool = True,
        bind: bool = True,
        versioned: bool = True,
        type_check: bool = True,
        metadata: Optional[typing.Dict[str, Any]] = None,
        storage_path: Optional[str] = None
    ):
        if path is not None:
            name, namespace = resolve_path(path)
        else:
            name = namespace = None

        def on_create():
            self.__args = ()
            self.__kwargs = {}
            self.__traces = Array('/'.join([
                constants.TASK_NAMESPACE,
                '__{0}__'.format(str(uuid.uuid4()))
            ]))
            self.__latest = Array('/'.join([
                constants.TASK_NAMESPACE,
                '__{0}__'.format(str(uuid.uuid4()))
            ]))
            self.__code = Dict('/'.join([
                constants.TASK_NAMESPACE,
                '__{0}__'.format(str(uuid.uuid4()))
            ]))

        if name:
            if namespace and namespace.startswith(constants.TASK_NAMESPACE) and '/' in namespace:
                if namespace.startswith(''.join([constants.TASK_NAMESPACE, '/'])):
                    name = '/'.join([namespace[len(constants.TASK_NAMESPACE) + 1:], name])
                else:
                    name = '/'.join([namespace, name])
            elif namespace and not namespace.startswith(constants.TASK_NAMESPACE):
                name = '/'.join([namespace, name])

        Entity.__init__(
            self, name, properties = [{}, {}], namespace = constants.TASK_NAMESPACE,
            create = create, bind = bind, versioned = versioned, on_create = on_create,
            type_check = type_check, metadata = metadata, storage_path = storage_path
        )

        if target:
            self._target_function = target
            module = inspect.getmodule(self._target_function)
            if not hasattr(module, '__file__'):
                bytecode = cloudpickle.dumps(self._target_function)
                digest = 'bytecode-{0}'.format(
                    create_string_digest(bytecode)
                )
                with transaction(constants.TASK_NAMESPACE):
                    if digest not in self.__code.keys():
                        self.__code[digest] = bytecode
                    if not self.__latest or self.__latest[-1] != digest:
                        self.__latest.append(digest)
            else:
                assert isinstance(module, types.ModuleType)
                digest = module_observer.get_digest(
                    module.__name__,
                    self._target_function.__name__
                )
                with transaction(constants.TASK_NAMESPACE):
                    if digest not in self.__code.keys():
                        self.__code[digest] = (
                            module.__name__,
                            self._target_function.__name__
                        )
                    if not self.__latest or self.__latest[-1] != digest:
                        self.__latest.append(digest)

    @property
    def function(self) -> Optional[Callable[..., Any]]:
        return self._target_function

    @property
    def args(self) -> Optional[Tuple[Any]]:
        return self.__args

    @args.setter
    def args(self, value: Tuple[Any]):
        self.__args = value

    @property
    def kwargs(self) -> typing.Dict[str, Any]:
        return self.__kwargs

    @kwargs.setter
    def kwargs(self, value: typing.Dict[str, Any]):
        self.__kwargs = value

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
        return getattr(module, function_name).function

    def invoke(
        self,
        /, *,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[typing.Dict[str, Any]] = None
    ) -> Any:
        assert self.__latest
        target_digest = self.__latest[-1]
        args = self.__args if args is None else args
        kwargs = self.__kwargs if kwargs is None else kwargs
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
        process_queue = Queue(constants.TASK_QUEUE_PATH)
        with transaction(constants.TASK_NAMESPACE):
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
            process_queue.put_nowait((self, trace_index, args, kwargs))
            return AsyncTraces.AsyncTrace(self.__traces, trace_index)

    def drop(self):
        with transaction(constants.TASK_NAMESPACE):
            for record in self.__traces[:]:
                if record['status'] == 'running':
                    terminate_node(
                        record['node_uid'],
                        create_string_digest(self.storage_path)
                    )
            self.__traces.drop()
            self.__code.drop()
            self.__latest.drop()
            super().drop()
