# pylint: disable = attribute-defined-outside-init, protected-access
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

from parkit.adapters.arguments import Arguments
from parkit.adapters.asyncexecution import AsyncExecution
from parkit.adapters.object import Object
from parkit.adapters.observer import ModuleObserver
from parkit.storage.context import transaction_context
from parkit.storage.environment import get_environment_threadsafe
from parkit.storage.namespace import Namespace
from parkit.utility import (
    create_string_digest,
    envexists,
    getenv,
    setenv
)

logger = logging.getLogger(__name__)

module_observer = ModuleObserver()

class Task(Object):

    _target_function: Optional[Callable[..., Any]] = None

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        target: Optional[Callable[..., Any]] = None,
        default_sync: Optional[bool] = None,
        metadata: Optional[Dict[str, Any]] = None,
        site: Optional[str] = None,
    ):
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

        def on_init(create: bool):
            if create:
                self.__default_sync = default_sync if default_sync is not None else False
                self.__latest = None
                load_target()
            else:
                with transaction_context(self._Entity__env, write = True):
                    if default_sync is not None:
                        self.__default_sync = default_sync
                    load_target()

        super().__init__(
            path,
            on_init = on_init, versioned = False,
            metadata = metadata, site = site
        )

    @functools.lru_cache(None)
    def __bytecode_cache(self, target_digest: str) -> Callable[..., Any]:
        return cloudpickle.loads(self.__latest[1])

    @functools.lru_cache(None)
    def __module_cache(self, target_digest: str) -> Callable[..., Any]:
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
            return getattr(module, function_name)._target_function
        return getattr(module, function_name)

    def invoke(
        self,
        /, *,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> Any:
        assert self.__latest
        with transaction_context(self._Entity__env, write = False):
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

    def executions(self) -> Iterator[AsyncExecution]:
        _, env, _, _, _, _ = get_environment_threadsafe(
            self.storage_path,
            constants.EXECUTION_NAMESPACE
        )
        with transaction_context(env, write = False, buffers = False) as (txn, _, _):
            cursor = txn.cursor()
            namespace = Namespace(constants.EXECUTION_NAMESPACE, site = self.site)
            if cursor.set_range(self.uuid.encode('utf-8')):
                while True:
                    key = cursor.key().decode('utf-8')
                    if key.startswith(self.uuid):
                        name = key.split(':')[1]
                        try:
                            entity = namespace[name]
                            if isinstance(entity, AsyncExecution):
                                yield entity
                        except KeyError:
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
            try:
                setenv(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME, self.site_uuid)
                return AsyncExecution(
                    task = self,
                    arguments = Arguments(args = args, kwargs = kwargs),
                    site = self.site
                )
            finally:
                setenv(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME, None)
        return self.invoke(args = args, kwargs = kwargs)

def task(
    *args,
    name: Optional[str] = None,
    qualify_name: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
    default_sync: Optional[bool] = None,
    site: Optional[str] = None
) -> Union[Task, Callable[[Callable[..., Any]], Task]]:

    def setup(name, target):
        if not name:
            if qualify_name:
                name = '.'.join([target.__module__, target.__name__])
            else:
                name = target.__name__
        return Task(
            '/'.join([constants.MODULE_NAMESPACE, name]), target = target,
            default_sync = default_sync, metadata = metadata,
            site = site
        )

    target = None

    if args:
        target = args[0]
        return setup(name, target)

    def decorator(target):
        return setup(name, target)

    return decorator

def bind_task(
    name: str,
    site: Optional[str] = None
):
    return Task('/'.join([constants.MODULE_NAMESPACE, name]), site = site)

def create_task(
    target: Callable[..., Any],
    /, *,
    name: Optional[str] = None,
    qualify_name: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
    site: Optional[str] = None
) -> Task:
    if not name:
        if qualify_name:
            name = '.'.join([target.__module__, target.__name__])
        else:
            name = target.__name__
    return Task(
        '/'.join([constants.MODULE_NAMESPACE, name]),
        target = target, metadata = metadata, site = site
    )
