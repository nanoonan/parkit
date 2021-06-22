import datetime
import enum
import logging
import sys
import time
import uuid

from typing import (
    Any, Callable, Dict, Iterator, Optional, Tuple, Union
)

import dateparser

import parkit.constants as constants

from parkit.adapters.object import Object
from parkit.adapters.task import Task
from parkit.storage.context import transaction_context
from parkit.storage.namespace import Namespace
from parkit.utility import resolve_path

logger = logging.getLogger(__name__)

class Frequency(enum.Enum):
    NANOSECOND = 0
    MICROSECOND = 1
    MILLISECOND = 2
    SECOND = 3
    MINUTE = 4
    HOUR = 5
    DAY = 6
    WEEK = 7
    MONTH = 8
    YEAR = 9

class Scheduler(Object):

    def __init__(
        self,
        path: str,
        /, *,
        task: Optional[Task] = None,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        on_init: Optional[Callable[[bool], None]] = None,
        site_uuid: Optional[str] = None,
        create: bool = True,
        bind: bool = False
    ):
        self.__task: Task
        self.__args: Tuple[Any, ...]
        self.__kwargs: Dict[str, Any]

        namespace, _, _ = resolve_path(path)

        if namespace != constants.SCHEDULER_NAMESPACE:
            raise ValueError()

        def _on_init(created: bool):
            if created:
                if task is None:
                    raise ValueError()
                self.__task = task
                self.__args = args if args is not None else ()
                self.__kwargs = kwargs if kwargs is not None else {}
            if on_init:
                on_init(create)

        super().__init__(
            path, on_init = _on_init, site_uuid = site_uuid,
            create = create, bind = bind
        )

    @property
    def task(self) -> Task:
        return self.__task

    @property
    def args(self) -> Tuple[Any, ...]:
        return self.__args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self.__kwargs

    def cancel(self):
        self.drop()

def schedule(
    task: Task,
    *args,
    **kwargs
) -> Scheduler:
    path = '/'.join([constants.SCHEDULER_NAMESPACE, str(uuid.uuid4())])
    return Periodic(
        path,
        frequency = kwargs['frequency'] if 'frequency' in kwargs else None,
        period = kwargs['period'] if 'period' in kwargs else None,
        start = kwargs['start'] if 'start' in kwargs else None,
        max_times = kwargs['max_times'] if 'max_times' in kwargs else None,
        task = task,
        args = args,
        kwargs = {
            key: value for key, value in kwargs.items() \
            if key not in ['frequency', 'period', 'start', 'max_times']
        },
        site_uuid = task.site_uuid,
        create = True,
        bind = False
    )

def schedulers(site_uuid: Optional[str] = None) -> Iterator[Scheduler]:
    for scheduler in Namespace(constants.SCHEDULER_NAMESPACE, site_uuid = site_uuid):
        if isinstance(scheduler, Scheduler):
            yield scheduler

frequency_ns = {
    Frequency.SECOND.value: 1e9,
    Frequency.MINUTE.value: 1e9 * 60,
    Frequency.HOUR.value: 1e9 * 3600,
    Frequency.DAY.value: 1e9 * 86400,
    Frequency.WEEK.value: 1e9 * 604800
}

def get_interval(frequency: Frequency, period: float):
    if frequency.value in frequency_ns:
        return int(frequency_ns[frequency.value] * period)
    raise ValueError()

class Periodic(Scheduler):

    def __init__(
        self,
        path: str,
        /, *,
        task: Optional[Task] = None,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        frequency: Optional[Frequency] = None,
        period: Optional[float] = None,
        start: Optional[Union[str, datetime.datetime]] = None,
        max_times: Optional[int] = None,
        site_uuid: Optional[str] = None,
        create: bool = True,
        bind: bool = False
    ):
        self.__count: int
        self.__last_run_ns: Optional[int]
        self.__next_run_ns: Optional[int]
        self.__start: Optional[datetime.datetime]
        self.__start_ns: Optional[int]
        self.__period: Optional[float]
        self.__frequency: Frequency
        self.__max_times: int

        frequency = Frequency.MINUTE if frequency is None else frequency
        period = 1. if period is None else period
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

        def on_init(created: bool):
            if created:
                self.__count = 0
                self.__last_run_ns = None
                self.__next_run_ns = None
                self.__start = parsed_start
                self.__start_ns = int(parsed_start.timestamp() * 1e9) if parsed_start else None
                assert period is not None
                self.__period = period
                assert frequency is not None
                self.__frequency = frequency
                self.__max_times = max_times if max_times is not None else sys.maxsize

        super().__init__(
            path, task = task, args = args, kwargs = kwargs,
            site_uuid = site_uuid, on_init = on_init,
            create = create, bind = bind
        )

    @property
    def count(self) -> int:
        return self.__count

    @property
    def next_run(self) -> Optional[datetime.datetime]:
        if self.__next_run_ns is not None:
            return datetime.datetime.fromtimestamp(self.__next_run_ns / 1e9)
        if self.__start_ns is not None:
            return datetime.datetime.fromtimestamp(self.__start_ns / 1e9)
        return None

    @property
    def last_run(self) -> Optional[datetime.datetime]:
        if self.__last_run_ns is not None:
            return datetime.datetime.fromtimestamp(self.__last_run_ns / 1e9)
        return None

    @property
    def start(self) -> Optional[datetime.datetime]:
        return self._start

    def is_scheduled(self) -> bool:

        with transaction_context(self._env, write = True):

            now_ns = time.time_ns()

            if self.__count == self.__max_times:
                return False

            assert self.__frequency is not None and self.__period is not None
            interval_ns = get_interval(self.__frequency, self.__period)

            def get_next_ns(start_ns):
                relative_ns = now_ns - start_ns
                offset_ns = relative_ns % interval_ns
                return now_ns + (interval_ns - offset_ns)

            if self.__last_run_ns is None:
                if self.__start_ns is None or self.__start_ns <= now_ns:
                    self.__count += 1
                    self.__last_run_ns = now_ns
                    if self.__count < self.__max_times:
                        if self.__start_ns is None:
                            self.__start_ns = now_ns
                        else:
                            self.__start_ns = self.__start_ns
                        self.__next_run_ns = get_next_ns(self.__start_ns)
                    return True
                return False

            assert isinstance(self.__next_run_ns, int)
            if self.__next_run_ns <= now_ns:
                self.__count += 1
                self.__last_run_ns = now_ns
                if self.__count < self.__max_times:
                    self.__next_run_ns = get_next_ns(self.__start_ns)
                else:
                    self.__next_run_ns = None
                return True
            return False
