# pylint: disable = unused-argument
import functools
import importlib
import importlib.abc
import importlib.util
import inspect
import logging
import os
import pickle
import types

from typing import (
    Any, Dict, Callable, Iterator, Optional, Tuple, Union
)

import cloudpickle

import parkit.constants as constants

from parkit.adapters.asyncexecution import AsyncExecution
from parkit.adapters.object import Object
from parkit.adapters.fileobserver import FileObserver
from parkit.exceptions import ObjectNotFoundError
from parkit.storage.context import transaction_context
from parkit.storage.environment import get_environment_threadsafe
from parkit.storage.namespace import Namespace
from parkit.utility import (
    create_string_digest,
    envexists,
    getenv,
    resolve_path,
    setenv
)

logger = logging.getLogger(__name__)

file_observer = FileObserver()

class Task(Object):

    _target_function: Optional[Callable[..., Any]] = None

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        target: Optional[Callable[..., Any]] = None,
        default_sync: Optional[bool] = None,
        metadata: Optional[Dict[str, Any]] = None,
        site_uuid: Optional[str] = None,
        create: bool = True,
        bind: bool = True
    ):
        self.__latest: Optional[Tuple[str, Union[bytes, Tuple[str, str]]]]
        self.__default_sync: bool

        if target:
            module = inspect.getmodule(target)
            if not hasattr(module, '__file__'):
                bytecode = cloudpickle.dumps(target)
                digest = '-'.join([
                    'bytecode',
                    create_string_digest(bytecode)
                ])
            else:
                assert isinstance(module, types.ModuleType)
                digest = file_observer.get_digest(
                    module.__name__,
                    target.__name__
                )

        def load_target():
            if target:
                self._target_function = target
                if not hasattr(module, '__file__'):
                    if self.__latest is None or self.__latest[0] != digest:
                        self.__latest = (digest, bytecode)
                else:
                    if self.__latest is None or self.__latest[0] != digest:
                        self.__latest = (
                            digest, (
                                module.__name__,
                                self._target_function.__name__
                            )
                        )

        def on_init(created: bool):
            if created:
                self.__default_sync = default_sync if default_sync is not None else False
                self.__latest = None
                load_target()
            else:
                with transaction_context(self._env, write = True):
                    if default_sync is not None:
                        self.__default_sync = default_sync
                    load_target()

        super().__init__(
            path,
            on_init = on_init, metadata = metadata,
            site_uuid = site_uuid, create = create, bind = bind
        )

    @functools.lru_cache(None)
    def __bytecode_cache(self, target_digest: str) -> Callable[..., Any]:
        assert isinstance(self.__latest, tuple) and isinstance(self.__latest[1], bytes)
        return cloudpickle.loads(self.__latest[1])

    @functools.lru_cache(None)
    def __module_cache(self, target_digest: str) -> Callable[..., Any]:
        assert isinstance(self.__latest, tuple) and isinstance(self.__latest[1], tuple)
        module_name, function_name = self.__latest[1]
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
        if isinstance(getattr(module, function_name), Task):
            return getattr(module, function_name).function
        return getattr(module, function_name)

    def invoke(
        self,
        /, *,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Any:
        assert self.__latest is not None
        with transaction_context(self._env, write = False):
            target_digest, _ = self.__latest
            if target_digest.startswith('bytecode'):
                target = self.__bytecode_cache(target_digest)
            else:
                target = self.__module_cache(target_digest)
        try:
            restore = getenv(constants.SELF_ENVNAME, str) \
            if envexists(constants.SELF_ENVNAME) else None
            setenv(
                constants.SELF_ENVNAME,
                pickle.dumps(self, 0).decode()
            )
            args = () if args is None else args
            kwargs = {} if kwargs is None else kwargs
            return target(*args, **kwargs)
        finally:
            setenv(
                constants.SELF_ENVNAME,
                restore
            )

    @property
    def function(self) -> Optional[Callable[..., Any]]:
        return self._target_function

    @property
    def executions(self) -> Iterator[AsyncExecution]:
        _, env, _, _, _, _ = get_environment_threadsafe(
            self.storage_path,
            constants.EXECUTION_NAMESPACE,
            create = False
        )
        with transaction_context(env, write = False) as (txn, _, _):
            cursor = txn.cursor()
            namespace = Namespace(constants.EXECUTION_NAMESPACE, site_uuid = self.site_uuid)
            if cursor.set_range(self.uuid.encode('utf-8')):
                while True:
                    key_bytes = cursor.key()
                    key_bytes = bytes(key_bytes) if isinstance(key_bytes, memoryview) else key_bytes
                    key = key_bytes.decode('utf-8')
                    if key.startswith(self.uuid):
                        name = key.split(':')[1]
                        try:
                            entity = namespace.get(name)
                            if isinstance(entity, AsyncExecution):
                                yield entity
                        except (KeyError, ObjectNotFoundError):
                            pass
                        if cursor.next():
                            continue
                    break

    def __call__(
        self,
        *args,
        **kwargs
    ) -> Any:
        sync = kwargs['sync'] if 'sync' in kwargs else None
        kwargs = {
            key: value for key, value in kwargs.items() \
            if key not in ['sync']
        }
        if sync is None:
            sync = self.__default_sync
        if not sync:
            return AsyncExecution(
                task = self,
                args = args,
                kwargs = kwargs,
                site_uuid = self.site_uuid,
                create = True,
                bind = False
            )
        return self.invoke(args = args, kwargs = kwargs)

def task(
    *args,
    path: Optional[str] = None,
    fullpath: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
    default_sync: Optional[bool] = None,
    site_uuid: Optional[str] = None
) -> Union[Task, Callable[[Callable[..., Any]], Task]]:

    def setup(path, target):
        if not path:
            if fullpath:
                namespace = target.__module__.replace('.', '/')
                name = target.__name__
                path = '/'.join([constants.MODULE_NAMESPACE, namespace, name])
            else:
                name = target.__name__
                path = '/'.join([constants.MODULE_NAMESPACE, name])
        else:
            namespace, name, _ = resolve_path(path)
            path = '/'.join([constants.MODULE_NAMESPACE, namespace, name])
        return Task(
            path, target = target,
            default_sync = default_sync, metadata = metadata,
            site_uuid = site_uuid
        )

    target = None

    if args:
        target = args[0]
        return setup(path, target)

    def decorator(target):
        return setup(path, target)

    return decorator

# def bind_task(
#     name: str,
#     site_uuid: Optional[str] = None
# ):
#     return Task('/'.join([constants.MODULE_NAMESPACE, name]), site_uuid = site_uuid)

# def create_task(
#     target: Callable[..., Any],
#     /, *,
#     name: Optional[str] = None,
#     qualify_name: bool = False,
#     metadata: Optional[Dict[str, Any]] = None,
#     site_uuid: Optional[str] = None
# ) -> Task:
#     if not name:
#         if qualify_name:
#             name = '.'.join([target.__module__, target.__name__])
#         else:
#             name = target.__name__
#     return Task(
#         '/'.join([constants.MODULE_NAMESPACE, name]),
#         target = target, metadata = metadata, site_uuid = site_uuid
#     )
