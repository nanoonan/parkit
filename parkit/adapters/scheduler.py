# pylint: disable = invalid-name
import datetime
import enum
import logging
import math
import time
import typing
import uuid

from typing import (
    Any, Callable, Iterator, Optional, Tuple, Union
)

import dateparser

import parkit.constants as constants

from parkit.adapters.dict import Dict
from parkit.adapters.object import Object
from parkit.adapters.task import Task
from parkit.storage.context import transaction_context
from parkit.utility import resolve_path

logger = logging.getLogger(__name__)

class Scheduler(Object):

    def __init__(
        self,
        path: str,
        /, *,
        on_init: Optional[Callable[[bool], None]] = None,
        site: Optional[str] = None
    ):
        namespace, _ = resolve_path(path)

        if namespace != constants.SCHEDULER_NAMESPACE:
            raise ValueError()

        def on_init_(create: bool):
            if create:
                self.__tasks = Dict('/'.join([
                    constants.SCHEDULER_NAMESPACE,
                    '__{0}__'.format(str(uuid.uuid4()))
                ]), site = site)
            if on_init:
                on_init(create)

        super().__init__(
            path, on_init = on_init_, site = site
        )

    @property
    def tasks(self) -> Iterator[Task]:
        for task, args, kwargs in self.__tasks.values():
            yield task

    @property
    def scheduled(self) -> Iterator[Tuple[Task, Tuple[Any, ...], typing.Dict[str, Any]]]:
        for task, args, kwargs in self.__tasks.values():
            if self.is_scheduled(task):
                yield (task, args, kwargs)

    def __contains__(self, task: Task) -> bool:
        return task.uuid in self.__tasks

    def is_scheduled(self, _: Task) -> bool:
        return True

    def schedule(self, task: Task, *args, **kwargs):
        with transaction_context(self._Entity__env, write = True):
            if task.uuid not in self.__tasks:
                self.__tasks[task.uuid] = (task, args, kwargs)

    def unschedule(self, task: Task):
        with transaction_context(self._Entity__env, write = True):
            if task.uuid in self.__tasks:
                del self.__tasks[task.uuid]

    def drop(self):
        with transaction_context(self._Entity__env, write = True):
            self.__tasks.drop()
            super().drop()

class Frequency(enum.Enum):
    Nanosecond = 0
    Microsecond = 1
    Millisecond = 2
    Second = 3
    Minute = 4
    Hour = 5
    Day = 6
    Week = 7
    Month = 8
    Year = 9

frequency_ns = {
    Frequency.Second.value: 1e9,
    Frequency.Minute.value: 1e9 * 60,
    Frequency.Hour.value: 1e9 * 3600,
    Frequency.Day.value: 1e9 * 86400,
    Frequency.Week.value: 1e9 * 604800
}

def get_interval(frequency: Frequency, period: float):
    if frequency.value in frequency_ns:
        return int(frequency_ns[frequency.value] * period)
    raise ValueError()

class Periodic(Scheduler):

    class TaskState():

        def __init__(self):
            self.count = 0
            self.start_ns = None
            self.last_run_ns = None
            self.next_run_ns = None

    def __init__(
        self,
        path: str,
        /, *,
        frequency: Frequency = Frequency.Minute,
        period: float = 1,
        start: Optional[Union[str, datetime.datetime]] = None,
        max_times: Optional[int] = None,
        site: Optional[str] = None
    ):
        if not (max_times is None or max_times > 0):
            raise ValueError()
        if period <= 0:
            raise ValueError()
        if start:
            parsed_start = start if isinstance(start, datetime.datetime) else \
            dateparser.parse(start)
            if not parsed_start:
                raise ValueError()
        else:
            parsed_start = None

        def on_init(create: bool):
            if create:
                self.__start = parsed_start
                self.__start_ns = int(parsed_start.timestamp() * 1e9) if parsed_start else None
                self.__period = period
                self.__frequency = frequency
                self.__max_times = max_times if max_times is not None else math.inf
                self.__task_state = Dict('/'.join([
                    constants.SCHEDULER_NAMESPACE,
                    '__{0}__'.format(str(uuid.uuid4()))
                ]), site = site)

        super().__init__(path, site = site, on_init = on_init)

    def next_run(self, task: Task) -> Optional[datetime.datetime]:
        with transaction_context(self._Entity__env, write = False):
            if task.uuid not in self.__task_state:
                raise ValueError()
            state = self.__task_state[task.uuid]
            return datetime.datetime.fromtimestamp(state.next_run_ns / 1e9) \
            if state.next_run_ns is not None else None

    def last_run(self, task: Task) -> Optional[datetime.datetime]:
        with transaction_context(self._Entity__env, write = False):
            if task.uuid not in self.__task_state:
                raise ValueError()
            state = self.__task_state[task.uuid]
            return datetime.datetime.fromtimestamp(state.last_run_ns / 1e9) \
            if state.last_run_ns is not None else None

    def is_scheduled(self, task: Task) -> bool:

        now_ns = time.time_ns()

        with transaction_context(self._Entity__env, write = True):

            if task.uuid not in self.__task_state:
                raise ValueError()

            state = self.__task_state[task.uuid]

            if state.count == self.__max_times:
                return False

            interval_ns = get_interval(self.__frequency, self.__period)

            def get_next_ns(start_ns):
                relative_ns = now_ns - start_ns
                offset_ns = relative_ns % interval_ns
                return now_ns + (interval_ns - offset_ns)

            try:
                if state.last_run_ns is None:
                    if self.__start_ns is None or self.__start_ns <= now_ns:
                        state.count += 1
                        state.last_run_ns = now_ns
                        if state.count < self.__max_times:
                            if self.__start_ns is None:
                                state.start_ns = now_ns
                            else:
                                state.start_ns = self.__start_ns
                            state.next_run_ns = get_next_ns(state.start_ns)
                        return True
                    return False

                if state.next_run_ns <= now_ns:
                    state.count += 1
                    state.last_run_ns = now_ns
                    if state.count < self.__max_times:
                        state.next_run_ns = get_next_ns(state.start_ns)
                    else:
                        state.next_run_ns = None
                    return True
                return False
            finally:
                self.__task_state[task.uuid] = state

    def schedule(self, task: Task, *args, **kwargs):
        with transaction_context(self._Entity__env, write = True):
            super().schedule(task, *args, **kwargs)
            if task.uuid not in self.__task_state:
                self.__task_state[task.uuid] = Periodic.TaskState()

    def unschedule(self, task: Task):
        with transaction_context(self._Entity__env, write = True):
            if task.uuid in self.__task_state:
                del self.__task_state[task.uuid]
            super().unschedule(task)

    def drop(self):
        with transaction_context(self._Entity__env, write = True):
            self.__task_state.drop()
            super().drop()
