# pylint: disable = invalid-name, too-few-public-methods
import datetime
import enum
import logging
import uuid

from typing import (
    Optional, Tuple, Union
)

import dateparser

import parkit.constants as constants

from parkit.adapters.object import Object

logger = logging.getLogger(__name__)

class Scheduler(Object):

    def __init__(self):
        path = '/'.join([constants.TASK_NAMESPACE, '__{0}__'.format(str(uuid.uuid4()))])
        super().__init__(path)

    def check_schedule(self, now_ns: int) -> Tuple[bool, Optional[int]]:
        return (True, now_ns)

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

    def __init__(
        self,
        /, *,
        frequency: Frequency = Frequency.Minute,
        period: float = 1,
        start: Optional[Union[str, datetime.datetime]] = None,
        max_times: Optional[int] = None
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
        self.start = parsed_start
        self.period = period
        self.frequency = frequency
        self.max_times = max_times
        self.count = 0
        self._calibrated = False
        self._start_ns = int(parsed_start.timestamp() * 1e9) if parsed_start else None

    def check_schedule(self, now_ns: int) -> Tuple[bool, Optional[int]]:

        if self.count == self.max_times:
            return (False, None)

        interval_ns = get_interval(self.frequency, self.period)

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
                self.count += 1
                return (True, now_ns + interval_ns)
            return (False, get_next_ns())
        self.count += 1
        return (True, get_next_ns())
