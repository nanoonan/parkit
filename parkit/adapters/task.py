import logging

from typing import (
    Any, Callable, Dict, Optional, Union
)

import parkit.constants as constants

from parkit.adapters.process import Process

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
        metadata: Optional[Dict[str, Any]] = None
    ):

        def on_init(create: bool):
            if create:
                self.__default_sync = default_sync if default_sync is not None else False
            else:
                if default_sync is not None:
                    self.__default_sync = default_sync

        super().__init__(
            path, target = target,
            create = create, bind = bind,
            site = site, metadata = metadata,
            on_init = on_init
        )

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
    metadata: Optional[Dict[str, Any]] = None,
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
