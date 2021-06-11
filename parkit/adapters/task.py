import logging
import typing
import uuid

from typing import (
    Any, Callable, Iterator, List, Optional, Tuple, Union
)

import parkit.constants as constants

from parkit.adapters.dict import Dict
from parkit.adapters.process import Process
from parkit.adapters.scheduler import Scheduler
from parkit.storage.context import transaction_context

logger = logging.getLogger(__name__)

class Task(Process):

    def __init__(
        self,
        path: str,
        /, *,
        target: Optional[Callable[..., Any]] = None,
        create: bool = False,
        bind: bool = True,
        site: Optional[str] = None,
        default_sync: Optional[bool] = None,
        metadata: Optional[typing.Dict[str, Any]] = None
    ):
        super().__init__(
            path, target = target,
            create = create, bind = bind,
            site = site, metadata = metadata
        )
        if '_Task__default_sync' not in self.attributes():
            self.__default_sync = default_sync if default_sync is not None else False
        else:
            if default_sync is not None:
                self.__default_sync = default_sync
        if metadata is not None:
            self.metadata = metadata
        if '_Task__schedulers' not in self.attributes():
            self.__schedulers = Dict('/'.join([
                constants.TASK_NAMESPACE,
                '__{0}__'.format(str(uuid.uuid4()))
            ]), site = site)

    @property
    def schedulers(self) -> List[Scheduler]:
        schedulers = []
        for value in self.__schedulers.values():
            schedulers.append(value[0])
        return schedulers

    def get_args(self, scheduler: Scheduler) \
    -> Optional[Tuple[Tuple[Any, ...], typing.Dict[str, Any]]]:
        try:
            _, args, kwargs = self.__schedulers[scheduler.uuid]
            return (args, kwargs)
        except KeyError:
            return None

    def schedule(
        self,
        scheduler: Scheduler,
        *args,
        **kwargs
    ) -> bool:
        with transaction_context(self._Entity__env, write = True):
            if scheduler not in self.__schedulers:
                self.__schedulers[scheduler.uuid] = (scheduler, args, kwargs)
                return True
            return False

    def unschedule(self, scheduler: Scheduler):
        try:
            del self.__schedulers[scheduler.uuid]
        except KeyError:
            pass

    def drop(self):
        with transaction_context(self._Entity__env, write = True):
            self.__schedulers.drop()
            super().drop()

    def __call__(
        self,
        *args,
        sync: Optional[bool] = None,
        **kwargs
    ) -> Any:
        if sync is None:
            sync = self.__default_sync
        if not sync:
            return self.submit(args = args, kwargs = kwargs)
        return self.invoke(args = args, kwargs = kwargs)

def task(
    *args,
    name: Optional[str] = None,
    qualify_name: bool = False,
    metadata: Optional[typing.Dict[str, Any]] = None,
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
            '/'.join([constants.TASK_NAMESPACE, name]), target = target,
            create = True, bind = True,
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
    return Task('/'.join([constants.TASK_NAMESPACE, name]), site = site)

def create_task(
    target: Callable[..., Any],
    /, *,
    name: Optional[str] = None,
    qualify_name: bool = False,
    metadata: Optional[typing.Dict[str, Any]] = None,
    site: Optional[str] = None
) -> Task:
    if not name:
        if qualify_name:
            name = '.'.join([target.__module__, target.__name__])
        else:
            name = target.__name__
    return Task(
        '/'.join([constants.TASK_NAMESPACE, name]),
        target = target, create = True, bind = True,
        metadata = metadata, site = site
    )
