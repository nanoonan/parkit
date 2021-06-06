# pylint: disable = invalid-name
import datetime
import enum
import logging
import uuid

from typing import (
    Any, Optional, Protocol, Tuple, Union
)

import dateparser

logger = logging.getLogger(__name__)

class Scheduler(Protocol):

    def check_schedule(self, now_ns: int) -> Tuple[bool, Optional[int]]:
        """Check for scheduling event"""

    @property
    def uuid(self) -> str:
        """Unique instance identifier"""

class SchedulerBase():

    def __init__(self):
        self._uuid = uuid.uuid4()

    def __hash__(self) -> int:
        return int.from_bytes(self._uuid.bytes, 'little')

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, SchedulerBase):
            return self._uuid == other._uuid
        return False

    @property
    def uuid(self) -> str:
        return str(self._uuid)

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

class Periodic(SchedulerBase):

    def __init__(
        self,
        /, *,
        frequency: Frequency = Frequency.Minute,
        period: float = 1,
        start: Optional[Union[str, datetime.datetime]] = None,
        max_times: Optional[int] = None,
    ):
        assert max_times is None or max_times >= 0
        assert period > 0
        if start:
            parsed_start = start if isinstance(start, datetime.datetime) else \
            dateparser.parse(start)
            if not parsed_start:
                raise ValueError()
        else:
            parsed_start = None
        super().__init__()
        self._start = parsed_start
        self._period = period
        self._frequency = frequency
        self._max_times = max_times
        self._count = 0
        self._calibrated = False
        self._start_ns = int(parsed_start.timestamp() * 1e9) if parsed_start else None

    @property
    def start(self) -> Optional[datetime.datetime]:
        return self._start

    @property
    def period(self) -> float:
        return self._period

    @property
    def frequency(self) -> Frequency:
        return self._frequency

    @property
    def max_times(self) -> Optional[int]:
        return self._max_times

    def check_schedule(self, now_ns: int) -> Tuple[bool, Optional[int]]:

        if self._count == self._max_times:
            return (False, None)

        interval_ns = get_interval(self._frequency, self._period)

        def get_next_ns():
            relative_ns = now_ns - self._start_ns
            offset_ns = relative_ns % interval_ns
            return now_ns + (interval_ns - offset_ns)

        if self._start_ns is not None and self._start_ns > now_ns:
            return (False, self._start_ns - now_ns)
        if not self._calibrated:
            self._calibrated = True
            if self._start_ns is None:
                self._start_ns = now_ns
            if self._start_ns == now_ns:
                self._count += 1
                return (True, now_ns + interval_ns)
            return (False, get_next_ns())
        self._count += 1

        return (True, get_next_ns())
