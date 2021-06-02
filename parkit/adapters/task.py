import logging

from typing import (
    Any, Callable, Optional, Union
)

import parkit.constants as constants

from parkit.adapters.function import Function
from parkit.adapters.scheduler import (
    Scheduler
)
from parkit.storage import (
    snapshot,
    transaction
)

logger = logging.getLogger(__name__)

class Task(Function):

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        target: Optional[Callable[..., Any]] = None,
        scheduler: Optional[Scheduler] = None,
        create: bool = False,
        bind: bool = True,
        storage_path: Optional[str] = None,
        default_sync: bool = False
    ):
        super().__init__(
            path, target = target, create = create, bind = bind,
            storage_path = storage_path
        )
        if '_default_sync' not in self.attributes():
            self._default_sync = default_sync
        if '_schedule' not in self.attributes():
            self._schedule = False
        if '_scheduler' not in self.attributes():
            self._scheduler = scheduler
        elif scheduler:
            self.scheduler = scheduler

    @property
    def schedule(self) -> bool:
        return self._schedule

    @schedule.setter
    def schedule(self, value: bool):
        assert isinstance(value, bool)
        self._schedule = value

    @property
    def scheduler(self) -> Optional[Scheduler]:
        return self._scheduler

    @scheduler.setter
    def scheduler(self, value: Optional[Scheduler]):
        assert value is None or isinstance(value, Scheduler)
        with transaction(constants.TASK_NAMESPACE):
            if self._scheduler:
                self._scheduler.drop()
            self._scheduler = value

    @property
    def scheduled(self) -> bool:
        with snapshot(constants.TASK_NAMESPACE):
            has_scheduler = self._scheduler is not None
            return self.schedule and has_scheduler

    def drop(self):
        with transaction(constants.TASK_NAMESPACE):
            if self._scheduler:
                self.scheduler.drop()
            super().drop()

    def __call__(
        self,
        sync: Optional[bool] = None,
        *args,
        **kwargs
    ) -> Any:
        if sync is None:
            sync = self._default_sync
        if not sync:
            return self.submit(args = args, kwargs = kwargs)
        return self.invoke(args = args, kwargs = kwargs)

def task(
    *args,
    default_sync: bool = False,
    name: Optional[str] = None,
    scheduler: Optional[Scheduler] = None,
) -> Union[Task, Callable[[Callable[..., Any]], Task]]:

    def setup(target):
        if name is None:
            name = '.'.join([target.__module__, target.__name__])
        return Task(
            name, target = target, scheduler = scheduler,
            create = True, bind = True, default_sync = default_sync
        )

    target = None

    if args:
        target = args[0]
        return setup(target)

    def decorator(target):
        return setup(target)

    return decorator

def bind_task(name: str):
    return Task(name)

def create_task(
    target: Callable[..., Any],
    /, *,
    name: Optional[str] = None,
    scheduler: Optional[Scheduler] = None
) -> Task:
    return Task(
        name, target = target, create = True, bind = False,
        scheduler = scheduler
    )
