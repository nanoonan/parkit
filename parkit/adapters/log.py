# pylint: disable = broad-except, no-value-for-parameter, non-parent-init-called, super-init-not-called, unused-import
import logging
import pickle
import struct

from typing import (
    Any, ByteString, Callable, cast, Iterator, Optional, Tuple, Union
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.object import ObjectMeta
from parkit.adapters.sized import Sized
from parkit.storage import (
    Entity,
    context,
    Missing
)
from parkit.utility import (
    compile_function,
    getenv,
    polling_loop,
    resolve_path
)

logger = logging.getLogger(__name__)

class ReversibleGetSlice():

    def __init__(
        self,
        owner,
        start: Optional[int],
        stop: Optional[int]
    ):
        self._owner = owner
        self._start = start
        self._stop = stop

    def __reversed__(self) -> Iterator[Any]:
        return self._owner.__reversed__(
            start = self._start, stop = self._stop
        )

    def __iter__(self) -> Iterator[Any]:
        return self._owner.__iter__(
            start = self._start, stop = self._stop
        )

def mkcontains(return_bool: bool = True) -> Tuple[str, Callable[..., Union[int, bool]]]:
    code = """
def method(
    self,
    value: Any,
    **kwargs: Dict[str, Optional[int]]
) -> Union[int, bool]:
    start = 0 if 'start' not in kwargs or kwargs['start'] is None else kwargs['start']
    end = 2**64 - 1 if 'end' not in kwargs or kwargs['end'] is None else kwargs['end']
    result: Tuple[bool, Optional[int]] = (False, None)
    if start <= end:
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._Entity__userdb[0])]
            if not cursor:
                txn = self._Entity__env.begin()
                cursor = txn.cursor(db = self._Entity__userdb[0])
            if cursor.set_range(struct.pack('@N', start)):
                while True:
                    key = struct.unpack('@N', cursor.key())[0]
                    if start <= key <= end:
                        curval = self.decitemval(cursor.value()) if self.decitemval else cursor.value()
                        if value == curval:
                            result = (True, key)
                            break
                    else:
                        break
                    if not cursor.next():
                        break
            if txn:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn)
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
    return result[1]
    """.strip()
    return (code.format(insert), compile_function(
        code, insert, glbs = globals()
    ))

def mkiter(reverse: bool = False) -> Tuple[str, Callable[..., Iterator[Any]]]:
    code = """
def method(
    self,
    **kwargs: Dict[str, Optional[int]]
) -> Iterator[Any]:
    start = 0 if 'start' not in kwargs or kwargs['start'] is None else kwargs['start']
    stop = 2**64 - 1 if 'stop' not in kwargs or kwargs['stop'] is None else kwargs['stop']
    with context(
        self._Entity__env, write = False,
        inherit = True, buffers = True
    ):
        cursor = thread.local.cursors[id(self._Entity__userdb[0])]
        size = thread.local.transaction.stat(self._Entity__userdb[0])['entries']
        if start < 0:
            start = size - abs(start)
        if stop < 0:
            stop = size - abs(stop)
        stop -= 1
        if stop < start:
            return
        {0}
        while True:
            item = cursor.value()
            key = struct.unpack('@N', cursor.key())[0]
            {1}
            yield self.decitemval(item) if self.decitemval else item
            {2}
"""
    insert0 = """
        stop = min(size - 1, stop)
        if not cursor.set_range(struct.pack('@N', stop)):
            return
    """.strip() if reverse else """
        start = max(0, start)
        if not cursor.set_range(struct.pack('@N', start)):
            return
    """.strip()
    insert1 = """
            if key < start:
                return
    """.strip() if reverse else """
            if key > stop:
                return
    """.strip()
    insert2 = """
            if not cursor.prev():
                return
    """.strip() if reverse else """
            if not cursor.next():
                return
    """.strip()
    return (code.format(insert0, insert1, insert2), compile_function(
        code, insert0, insert1, insert2, glbs = globals()
    ))

class LogMeta(ObjectMeta):

    def __initialize_class__(cls):
        method: Any
        if isinstance(cast(Log, cls).__contains__, Missing):
            code, method = mkcontains(return_bool = True)
            setattr(cls, '__contains__', method)
            setattr(cls, '__contains__code', code)
        if isinstance(cast(Log, cls).index, Missing):
            code, method = mkcontains(return_bool = False)
            setattr(cls, 'index', method)
            setattr(cls, 'indexcode', code)
        if isinstance(cast(Log, cls).__iter__, Missing):
            code, method = mkiter(reverse = False)
            setattr(cls, '__iter__', method)
            setattr(cls, '__iter__code', code)
        if isinstance(cast(Log, cls).__reversed__, Missing):
            code, method = mkiter(reverse = True)
            setattr(cls, '__reversed__', method)
            setattr(cls, '__reversed__code', code)
        super().__initialize_class__()

class Log(Sized, metaclass = LogMeta):

    decitemval: Optional[Callable[..., Any]] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encitemval: Optional[Callable[..., ByteString]] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    def __init__(
        self,
        path: str,
        /, *,
        create: bool = True,
        bind: bool = True
    ):
        name, namespace = resolve_path(path)
        Entity.__init__(
            self, name, properties = [{'integerkey': True}],
            namespace = namespace, create = create, bind = bind
        )

    def __getitem__(
        self,
        key: Union[int, slice],
        /
    ) -> Union[Any, ReversibleGetSlice]:
        if isinstance(key, slice):
            return ReversibleGetSlice(self, key.start, key.stop)
        try:
            implicit = False
            cursor = None
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin()
                cursor = txn.cursor(db = self._Entity__userdb[0])
            else:
                cursor = thread.local.cursors[id(self._Entity__userdb[0])]
            if key < 0:
                key = txn.stat(self._Entity__userdb[0])['entries'] + key
            result = cursor.get(key = struct.pack('@N', key)) if cast(int, key) >= 0 else None
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)
        finally:
            if implicit and cursor:
                cursor.close()
        if result is None:
            raise IndexError()
        return self.decitemval(result) if self.decitemval else result

    def append(
        self,
        item: Any,
        /
    ):
        item = self.encitemval(item) if self.encitemval else item
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._Entity__userdb[0])]
            if not cursor:
                txn = self._Entity__env.begin(write = True)
                cursor = txn.cursor(db = self._Entity__userdb[0])
            if not cursor.last():
                key = struct.pack('@N', 0)
            else:
                key = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)
            assert cursor.put(key = key, value = item, append = True)
            if txn:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn)
        finally:
            if txn and cursor:
                cursor.close()

    def wait(
        self,
        threshold: int,
        timeout: Optional[float] = None,
        polling_interval: Optional[float] = None
    ):
        default_polling_interval = getenv(constants.ADAPTER_POLLING_INTERVAL_ENVNAME, float)
        for _ in polling_loop(
            polling_interval if polling_interval is not None else \
            default_polling_interval,
            timeout = timeout
        ):
            if len(self) > threshold:
                break

    __contains__: Callable[..., bool] = Missing()

    index: Callable[..., int] = Missing()

    __reversed__: Callable[..., Iterator[Any]] = Missing()

    __iter__: Callable[..., Iterator[Any]] = Missing()

    count = Sized.__len__

class BytesLog(Log):
    encitemval = None
    decitemval = None
