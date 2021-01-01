# pylint: disable = broad-except, protected-access, no-value-for-parameter
import logging
import pickle
import struct
import time

from typing import (
    Any, ByteString, Callable, cast, Generator, Optional, Tuple, Union
)

import numpy as np
import pandas as pd

from tzlocal import get_localzone

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.sized import Sized
from parkit.exceptions import abort
from parkit.storage import (
    context,
    Missing,
    EntityMeta
)
from parkit.utility import (
    compile_function,
    polling_loop,
    resolve_path
)

logger = logging.getLogger(__name__)

_local_timezone = get_localzone()

def timestamp(nanos: int) -> pd.Timestamp:
    return pd.Timestamp(np.datetime64(nanos, 'ns'), tz = _local_timezone)

class ReversibleGetSlice():

    def __init__(
        self,
        owner,
        start: Optional[int],
        stop: Optional[int]
    ) -> None:
        self._owner = owner
        self._start = start
        self._stop = stop

    def __reversed__(self) -> Generator[Any, None, None]:
        return self._owner.__reversed__(
            self._start, self._stop
        )

    def __iter__(self) -> Generator[Any, None, None]:
        return self._owner.__iter__(
            self._start, self._stop
        )

def mkappend(use_timestamp: bool = False) -> Callable[..., None]:
    code = """
def method(
    self,
    value: Any
) -> None:
    value = self.encode_value(value) if self.encode_value else value
    try:
        txn = cursor = None
        cursor = thread.local.cursors[id(self._user_db[0])]
        if not cursor:
            txn = self._environment.begin(write = True)
            cursor = txn.cursor(db = self._user_db[0])
        {0}
        if txn:
            txn.commit()
    except BaseException as exc:
        if txn:
            txn.abort()
        abort(exc)
    finally:
        if txn and cursor:
            cursor.close()
"""
    insert = """
        key = struct.pack('@N', time.time_ns())
        cursor.put(key = key, value = value, dupdata = True)
    """.strip() if use_timestamp else """
        if not cursor.last():
            key = struct.pack('@N', 0)
        else:
            key = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)
        assert cursor.put(key = key, value = value, append = True)
    """
    return compile_function(
        code, insert,
        globals_dict = dict(
            struct = struct, thread = thread, id = id, BaseException = BaseException,
            abort = abort
        )
    )

def mkcontains(return_bool: bool = True) -> Callable[..., Union[int, bool]]:
    code = """
def method(
    self,
    value: Any,
    start: Optional[int] = None,
    end: Optional[int] = None
) -> Union[int, bool]:
    try:
        txn = cursor = None
        cursor = thread.local.cursors[id(self._user_db[0])]
        if not cursor:
            txn = self._environment.begin()
            cursor = txn.cursor(db = self._user_db[0])
        result: Tuple[bool, Optional[int]] = (False, None)
        if cursor.first():
            start = struct.unpack('@N', cursor.key())[0] if start is None else start
            cursor.last()
            end = struct.unpack('@N', cursor.key())[0] if end is None else end
            cursor.first()
            if start <= end:
                while True:
                    key = struct.unpack('@N', cursor.key())[0]
                    if start <= key <= end:
                        curval = self.decode_value(cursor.value()) if self.decode_value else cursor.value()
                        if value == curval:
                            result = (True, key)
                            break
                    if not cursor.next():
                        break
        if txn:
            txn.commit()
    except BaseException as exc:
        if txn:
            txn.abort()
        abort(exc)
    finally:
        if txn and cursor:
            cursor.close()
    {0}
"""
    insert = """
    return result[0]
    """.strip() if return_bool else """
    if result[1] is None:
        raise ValueError()
    return cast(int, result[1])
    """
    return compile_function(
        code, insert,
        globals_dict = dict(
            struct = struct, thread = thread, id = id, BaseException = BaseException,
            abort = abort, ValueError = ValueError
        )
    )

def mkiter(
    use_timestamp: bool = False,
    reverse: bool = False
) -> Callable[..., Any]:
    code = """
def method(
    self,
    start: Optional[int] = None,
    stop: Optional[int] = None,
) -> Generator[Any, None, None]:
    start = 0 if start is None or start < 0 else start
    {0}
    with context(
        self._environment, write = False,
        inherit = True, buffers = True
    ):
        cursor = thread.local.cursors[id(self._user_db[0])]

        if stop is None:
            if not cursor.last():
                return
            stop = struct.unpack('@N', cursor.key())[0]
        else:
            {1}
        {2}
        if stop < start:
            return
        while True:
            value = cursor.value()
            key = struct.unpack('@N', cursor.key())[0]
            {3}
            {4}
            {5}

"""
    insert0 = '' if reverse else """
    stop = 2**64 - 1 if stop is None else stop
    """.strip()
    insert1 = '' if use_timestamp else """
            if stop < 0:
                stop += thread.local.transaction.stat(self._user_db[0])['entries']
            stop = stop - 1
    """.strip()
    insert2 = """
        if not cursor.set_range(struct.pack('@N', stop)):
            return
    """.strip() if reverse else """
        if not cursor.set_range(struct.pack('@N', start)):
            return
    """
    insert3 = """
            if key < start:
                return
    """.strip() if reverse else """
            if key > stop:
                return
    """.strip()
    insert4 = """
            yield (key, self.decode_value(value) if self.decode_value else value)
    """.strip() if use_timestamp else """
            yield self.decode_value(value) if self.decode_value else value
    """.strip()
    insert5 = """
            if not cursor.prev():
                return
    """.strip() if reverse else """
            if not cursor.next():
                return
    """
    print(reverse, code.format(insert0, insert1, insert2, insert3, insert4, insert5))
    return compile_function(
        code, insert0, insert1, insert2, insert3, insert4, insert5,
        globals_dict = dict(
            struct = struct, thread = thread, id = id, context = context
        )
    )

def mkgetitem(use_timestamp: bool = False) -> Callable[..., Any]:
    code = """
def method(
    self,
    key: Union[int, pd.Timestamp, slice]
) -> Union[Any, ReversibleGetSlice]:
    if isinstance(key, slice):
        return ReversibleGetSlice(
            self,
            {0}
        )
    try:
        implicit = False
        cursor = None
        txn = thread.local.transaction
        if not txn:
            implicit = True
            txn = self._environment.begin()
            cursor = txn.cursor(db = self._user_db[0])
        else:
            cursor = thread.local.cursors[id(self._user_db[0])]
        {1}
        result = cursor.get(key = struct.pack('@N', key)) if key >= 0 else None
        if implicit:
            txn.commit()
    except BaseException as exc:
        if implicit and txn:
            txn.abort()
        abort(exc)
    finally:
        if implicit and cursor:
            cursor.close()
    if result is None:
        raise IndexError()
    {2}
"""
    insert0 = """
            (int(key.start.asm8) if key.start else None) if use_timestamp,
            (int(key.stop.asm8) if key.stop else None) if use_timestamp
    """.strip() if use_timestamp else """
            key.start,
            key.stop
    """
    insert1 = """
        key = int(cast(pd.Timestamp, key).asm8)
    """.strip() if use_timestamp else """
        if key < 0:
            key = txn.stat(self._user_db[0])['entries'] + key
    """.strip()
    insert2 = """
    return (key, self.decode_value(result)) if self.decode_value else (key, result)
    """.strip() if use_timestamp else """
    return self.decode_value(result) if self.decode_value else result
    """
    return compile_function(
        code, insert0, insert1, insert2,
        globals_dict = dict(
            struct = struct, BaseException = BaseException, thread = thread,
            abort = abort, id = id, IndexError = IndexError, pd = pd,
            ReversibleGetSlice = ReversibleGetSlice, slice = slice,
            isinstance = isinstance
        )
    )

class _Log(Sized):

    decode_value: Callable[..., Any] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encode_value: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    __contains__: Callable[..., bool] = Missing()

    index: Callable[..., int] = Missing()

    __getitem__: Callable[..., Any] = Missing()

    append: Callable[..., None] = Missing()

    __reversed__: Callable[..., Generator[Any, None, None]] = Missing()

    __iter__: Callable[..., Generator[Any, None, None]] = Missing()

    count = Sized.__len__

    def wait(
        self,
        threshold: int,
        timeout: Optional[float] = None,
        polling_interval: Optional[float] = None
    ):
        for _ in polling_loop(
            polling_interval if polling_interval is not None else \
            constants.DEFAULT_ADAPTER_POLLING_INTERVAL,
            timeout = timeout
        ):
            if len(self) > threshold:
                break

class LogMeta(EntityMeta):

    def __initialize_class__(cls):
        super().__initialize_class__()
        if isinstance(cls.__getitem__, Missing):
            setattr(cls, '__getitem__', mkgetitem(use_timestamp = False))
        if isinstance(cls.__contains__, Missing):
            setattr(cls, '__contains__', mkcontains(return_bool = True))
        if isinstance(cls.index, Missing):
            setattr(cls, 'index', mkcontains(return_bool = False))
        if isinstance(cls.append, Missing):
            setattr(cls, 'append', mkappend(use_timestamp = False))
        if isinstance(cls.__iter__, Missing):
            setattr(cls, '__iter__', mkiter(use_timestamp = False, reverse = False))
        if isinstance(cls.__reversed__, Missing):
            setattr(cls, '__reversed__', mkiter(use_timestamp = False, reverse = True))

    def __call__(cls, *args, **kwargs):
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)

class Log(_Log, metaclass = LogMeta):

    def __init__(
        self, path: str, create: bool = True, bind: bool = True
    ) -> None:
        name, namespace = resolve_path(path)
        super().__init__(
            name, properties = [{'integerkey': True}],
            namespace = namespace, create = create, bind = bind
        )

class TimestampLogMeta(EntityMeta):

    def __initialize_class__(cls):
        super().__initialize_class__()
        if isinstance(cls.__getitem__, Missing):
            setattr(cls, '__getitem__', mkgetitem(use_timestamp = True))
        if isinstance(cls.__contains__, Missing):
            setattr(cls, '__contains__', mkcontains(return_bool = True))
        if isinstance(cls.index, Missing):
            setattr(cls, 'index', mkcontains(return_bool = False))
        if isinstance(cls.append, Missing):
            setattr(cls, 'append', mkappend(use_timestamp = True))
        if isinstance(cls.__iter__, Missing):
            setattr(cls, '__iter__', mkiter(use_timestamp = True, reverse = False))
        if isinstance(cls.__reversed__, Missing):
            setattr(cls, '__reversed__', mkiter(use_timestamp = True, reverse = True))

    def __call__(cls, *args, **kwargs):
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)

class TimestampLog(_Log, metaclass = TimestampLogMeta):

    def __init__(
        self, path: str, create: bool = True, bind: bool = True
    ) -> None:
        name, namespace = resolve_path(path)
        super().__init__(
            name,
            properties = [{
                'integerkey': True,
                'dupsort': True
            }],
            namespace = namespace, create = create, bind = bind
        )
