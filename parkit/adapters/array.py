# pylint: disable = not-callable, broad-except, unused-import, no-self-use
import logging
import pickle
import struct

from typing import (
    Any, ByteString, Callable, cast, Dict, Iterable,
    Iterator, Optional, Tuple, Union
)

import parkit.storage.threadlocal as thread

from parkit.adapters.sized import Sized
from parkit.storage.context import transaction_context
from parkit.storage.entitymeta import (
    ClassBuilder,
    EntityMeta,
    Missing
)

from parkit.utility import compile_function

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

def mkcontains(return_bool: bool = True) -> Callable[..., Union[int, bool]]:
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
            txn, cursors, _, implicit = \
            thread.local.context.get(self._Entity__env, write = False, internal = True)
            cursor = cursors[self._Entity__userdb[0]]
            if cursor.set_range(struct.pack('@N', start)):
                while True:
                    key = struct.unpack('@N', cursor.key())[0]
                    if start <= key <= end:
                        curval = (
                            self.decode_value(
                                cursor.value(),
                                pickle.loads(txn.get(key = cursor.key(), db = self._Entity__userdb[1]))
                            ) if self.get_metadata else self.decode_value(cursor.value())
                        ) if self.decode_value else cursor.value()
                        if value == curval:
                            result = (True, key)
                            break
                    else:
                        break
                    if not cursor.next():
                        break
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        finally:
            if implicit and cursor:
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
    return compile_function(
        code.format(insert), glbs = globals()
    )

def mkiter(reverse: bool = False) -> Callable[..., Iterator[Any]]:
    code = """
def method(
    self,
    **kwargs: Dict[str, Optional[int]]
) -> Iterator[Any]:
    start = 0 if 'start' not in kwargs or kwargs['start'] is None else kwargs['start']
    stop = 2**64 - 1 if 'stop' not in kwargs or kwargs['stop'] is None else kwargs['stop']
    with transaction_context(self._Entity__env, write = False, iterator = True) as (txn, cursors, _):
        cursor = cursors[self._Entity__userdb[0]]
        size = txn.stat(self._Entity__userdb[0])['entries']
        if start < 0:
            start = size - abs(start)
        if stop < 0:
            stop = size - abs(stop)
        stop -= 1
        if stop < start:
            return
        {0}
        while True:
            key = struct.unpack('@N', cursor.key())[0]
            {1}
            yield (
                self.decode_value(cursor.value(), pickle.loads(txn.get(key = cursor.key(), db = self._Entity__userdb[1]))) \
                if self.get_metadata else self.decode_value(cursor.value())
            ) if self.decode_value else cursor.value()
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
    return compile_function(
        code.format(insert0, insert1, insert2), glbs = globals()
    )

class ArrayMeta(ClassBuilder):

    def __build_class__(cls, target, attr):
        if target == Array:
            if attr == '__contains__':
                setattr(target, '__contains__', mkcontains(return_bool = True))
            elif attr == 'index':
                setattr(target, 'index', mkcontains(return_bool = False))
            elif attr == '__iter__':
                setattr(target, '__iter__', mkiter(reverse = False))
            elif attr == '__reversed__':
                setattr(target, '__reversed__', mkiter(reverse = True))

class Array(Sized, metaclass = ArrayMeta):

    get_metadata: Optional[Callable[..., Any]] = None

    decode_value: Optional[Callable[..., Any]] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encode_value: Optional[Callable[..., ByteString]] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        metadata: Optional[Dict[str, Any]] = None,
        site: Optional[str] = None,
        on_init: Optional[Callable[[bool], None]] = None
    ):
        super().__init__(
            path, db_properties = [{'integerkey': True}, {'integerkey': True}],
            on_init = on_init, metadata = metadata, site = site
        )

    def __getitem__(
        self,
        key: Union[int, slice],
        /
    ) -> Any:
        if isinstance(key, slice):
            return ReversibleGetSlice(self, key.start, key.stop)
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self._Entity__env, write = False, internal = True)

            cursor = cursors[self._Entity__userdb[0]]

            if key < 0:
                key = txn.stat(self._Entity__userdb[0])['entries'] + key

            if cast(int, key) >= 0:
                key_bytes = struct.pack('@N', key)
                data = cursor.get(key = key_bytes)
                meta = pickle.loads(txn.get(key = key_bytes, db = self._Entity__userdb[1])) \
                if self.get_metadata else None
            else:
                data = None

            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        finally:
            if implicit and cursor:
                cursor.close()
        if data is None:
            raise IndexError()
        return (self.decode_value(data, meta) if self.get_metadata else self.decode_value(data)) \
        if self.decode_value else data

    def count(self, value: Any) -> int:
        count = 0
        for stored in self[:]:
            if stored == value:
                count += 1
        return count

    def __setitem__(
        self,
        key: int,
        value: Any,
        /
    ):
        meta = pickle.dumps(self.get_metadata(value)) if self.get_metadata else None
        value_bytes = self.encode_value(value) if self.encode_value else value
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True, internal = True)

            cursor = cursors[self._Entity__userdb[0]]

            key_bytes = struct.pack('@N', key)

            assert cursor.put(key = key_bytes, value = value_bytes, append = False)
            if self.get_metadata:
                assert txn.put(
                    key = key_bytes, value = meta, append = False,
                    db = self._Entity__userdb[1]
                )

            if implicit:
                self._Entity__increment_version(cursors)
                txn.commit()
            else:
                changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)

    def append(
        self,
        item: Any,
        /
    ):
        meta = pickle.dumps(self.get_metadata(item)) if self.get_metadata else None
        item_bytes = self.encode_value(item) if self.encode_value else item
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True, internal = True)

            cursor = cursors[self._Entity__userdb[0]]

            if not cursor.last():
                key_bytes = struct.pack('@N', 0)
            else:
                key_bytes = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)
            assert cursor.put(key = key_bytes, value = item_bytes, append = True)
            if self.get_metadata:
                assert txn.put(
                    key = key_bytes, value = meta, append = True, db = self._Entity__userdb[1]
                )
            if implicit:
                self._Entity__increment_version(cursors)
                txn.commit()
            else:
                changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)

    def extend(
        self,
        items: Iterable[Any],
        /
    ):
        packed = []
        for item in items:
            meta = pickle.dumps(self.get_metadata(item)) if self.get_metadata else None
            item_bytes = self.encode_value(item) if self.encode_value else item
            packed.append((item_bytes, meta))
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True, internal = True)

            cursor = cursors[self._Entity__userdb[0]]

            if not cursor.last():
                key = 0
            else:
                key = struct.unpack('@N', cursor.key())[0] + 1

            for item, meta in packed:
                key_bytes = struct.pack('@N', key)
                assert cursor.put(key = key_bytes, value = item, append = True)
                if meta is not None:
                    assert txn.put(
                        key = key_bytes, value = meta, append = True, db = self._Entity__userdb[1]
                    )
                key += 1

            if implicit:
                self._Entity__increment_version(cursors)
                txn.commit()
            else:
                changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)

    def __bool__(self) -> bool:
        return len(self) > 0

    __contains__: Callable[..., bool] = Missing()

    index: Callable[..., int] = Missing()

    __reversed__: Callable[..., Iterator[Any]] = Missing()

    __iter__: Callable[..., Iterator[Any]] = Missing()
