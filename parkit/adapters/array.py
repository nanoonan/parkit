# pylint: disable = broad-except, no-self-use, not-callable, unused-import
import logging
import math
import pickle
import struct

from typing import (
    Any, ByteString, Callable, cast, Dict, Iterable,
    Iterator, List, Optional, Tuple, Union
)

import parkit.storage.threadlocal as thread

from parkit.adapters.sized import Sized
from parkit.storage.context import transaction_context
from parkit.storage.entitymeta import (
    ClassBuilder,
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

def mkiter(reverse: bool = False) -> Callable[..., Iterator[Any]]:
    code = """
def method(
    self,
    **kwargs: Dict[str, Optional[int]]
) -> Iterator[Any]:
    start = 0 if 'start' not in kwargs or kwargs['start'] is None else kwargs['start']
    stop = 2**64 - 1 if 'stop' not in kwargs or kwargs['stop'] is None else kwargs['stop']
    with transaction_context(self._env, write = False, iterator = True) as (txn, cursors, _):
        cursor = cursors[self._userdb[0]]
        if cursor.first():
            key_start = struct.unpack('@N', cursor.key())[0]
            size = txn.stat(self._userdb[0])['entries']
            if start < 0:
                start = size - abs(start)
            if stop < 0:
                stop = size - abs(stop)
            stop -= 1
            if stop < start:
                return
            start += key_start
            stop += key_start
            {0}
            while True:
                key = struct.unpack('@N', cursor.key())[0]
                {1}
                yield (
                    self.decode_value(cursor.value(), pickle.loads(txn.get(key = cursor.key(), db = self._userdb[1]))) \
                    if self.get_metadata else self.decode_value(cursor.value())
                ) if self.decode_value else cursor.value()
                {2}
"""
    insert0 = """
            stop = min(size + key_start - 1, stop)
            if not cursor.set_range(struct.pack('@N', stop)):
                return
    """.strip() if reverse else """
            start = max(key_start, start)
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
            if attr == '__iter__':
                setattr(target, '__iter__', mkiter(reverse = False))
            elif attr == '__reversed__':
                setattr(target, '__reversed__', mkiter(reverse = True))

class Array(Sized, metaclass = ArrayMeta):

    _maxsize_cached = math.inf

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
        site_uuid: Optional[str] = None,
        on_init: Optional[Callable[[bool], None]] = None,
        create: bool = False,
        bind: bool = True,
        maxsize: int = 0
    ):
        self.__maxsize: float

        def _on_init(created: bool):
            if created:
                self.__maxsize = maxsize if maxsize > 0 else math.inf
            if on_init:
                on_init(create)

        super().__init__(
            path, db_properties = [{'integerkey': True}, {'integerkey': True}],
            on_init = _on_init, metadata = metadata, site_uuid = site_uuid,
            create = create, bind = bind
        )

        self._maxsize_cached = self.__maxsize

    def __setstate__(self, from_wire: Tuple[str, str, str]):
        super().__setstate__(from_wire)
        self._maxsize_cached = self.__maxsize

    @property
    def maxsize(self) -> Optional[int]:
        return int(self._maxsize_cached) if self._maxsize_cached != math.inf else None

    def __getitem__(
        self,
        key: Union[int, slice],
        /
    ) -> Any:
        if isinstance(key, slice):
            return ReversibleGetSlice(self, key.start, key.stop)
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self._env, write = False, internal = True)

            cursor = cursors[self._userdb[0]]

            if key < 0:
                key = txn.stat(self._userdb[0])['entries'] + key

            assert isinstance(key, int)
            if key >= 0 and cursor.first():
                key_start = struct.unpack('@N', cursor.key())[0]
                key_bytes = struct.pack('@N', key + key_start)
                data = cursor.get(key = key_bytes)
                if data is not None:
                    meta = txn.get(key = key_bytes, db = self._userdb[1]) \
                    if self.get_metadata else None
            else:
                data = meta = None

            if implicit:
                txn.commit()
        except BaseException as exc:
            self._abort(exc, txn, implicit)
        finally:
            if implicit and cursor:
                cursor.close()
        if data is None:
            raise IndexError()
        return (self.decode_value(data, pickle.loads(meta)) \
        if self.get_metadata else self.decode_value(data)) \
        if self.decode_value else data

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
            thread.local.context.get(self._env, write = True, internal = True)

            cursor = cursors[self._userdb[0]]

            result = False
            if cursor.first():
                key_start = struct.unpack('@N', cursor.key())[0]
                key_bytes = struct.pack('@N', key + key_start)
                if cursor.set_key(key_bytes):
                    assert cursor.put(key = key_bytes, value = value_bytes, append = False)
                    if self.get_metadata:
                        assert txn.put(
                            key = key_bytes, value = meta, append = False,
                            db = self._userdb[1]
                        )
                    result = True
                    if implicit:
                        self._increment_version(cursors)
                    else:
                        changed.add(self)

            if implicit:
                txn.commit()
        except BaseException as exc:
            self._abort(exc, txn, implicit)
        if not result:
            raise IndexError()

    def __pop(self, left: bool = False) -> Any:
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._env, write = True, internal = True)
            cursor = cursors[self._userdb[0]]
            data = meta = None
            if cursor.first() if left else cursor.last():
                key = cursor.key()
                data = cursor.pop(key = key)
                if data is not None:
                    if self.get_metadata:
                        meta = txn.pop(
                            key = key,
                            db = self._userdb[1]
                        )
                        assert meta is not None
                    if implicit:
                        self._increment_version(cursors)
                    else:
                        changed.add(self)
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._abort(exc, txn, implicit)
        if data is None:
            raise IndexError()
        return (self.decode_value(data, pickle.loads(cast(bytes, meta))) \
        if self.get_metadata else self.decode_value(data)) \
        if self.decode_value else data

    def pop(self) -> Any:
        return self.__pop(left = False)

    def popleft(self) -> Any:
        return self.__pop(left = True)

    def append(
        self,
        item: Any,
        /
    ):
        meta = pickle.dumps(self.get_metadata(item)) if self.get_metadata else None
        item_bytes = self.encode_value(item) if self.encode_value else item
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._env, write = True, internal = True)

            curlen = txn.stat(self._userdb[0])['entries']
            assert curlen <= self._maxsize_cached

            cursor = cursors[self._userdb[0]]

            if not cursor.last():
                key_bytes = struct.pack('@N', 0)
            else:
                key_bytes = struct.pack(
                    '@N',
                    struct.unpack('@N', cursor.key())[0] + 1
                )
            if curlen == self._maxsize_cached:
                assert cursor.first()
                key = cursor.key()
                assert cursor.delete(key)
                if self.get_metadata:
                    assert txn.delete(key = key, db = self._userdb[1])
            assert cursor.put(
                key = key_bytes, value = item_bytes,
                append = True
            )
            if self.get_metadata:
                assert txn.put(
                    key = key_bytes, value = meta,
                    append = True, db = self._userdb[1]
                )

            if implicit:
                self._increment_version(cursors)
                txn.commit()
            else:
                changed.add(self)
        except BaseException as exc:
            self._abort(exc, txn, implicit)

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
        packed = packed[
            0 if len(packed) <= self._maxsize_cached else int(len(packed) - self._maxsize_cached):
        ]
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._env, write = True, internal = True)

            curlen = txn.stat(self._userdb[0])['entries']
            assert curlen <= self._maxsize_cached

            data_cursor = cursors[self._userdb[0]]
            meta_cursor = cursors[self._userdb[1]]

            if not data_cursor.last():
                key = 0
            else:
                key = struct.unpack('@N', data_cursor.key())[0] + 1

            values_multi = []
            meta_multi: Optional[List[Tuple[bytes, Optional[bytes]]]] = [] \
            if self.get_metadata else None

            for item_bytes, meta in packed:
                key_bytes = struct.pack('@N', key)
                values_multi.append((key_bytes, item_bytes))
                if meta_multi is not None:
                    assert meta is not None
                    meta_multi.append((key_bytes, meta))
                key += 1

            if curlen + len(values_multi) > self._maxsize_cached:
                assert data_cursor.first()
                if self.get_metadata:
                    assert meta_cursor.first()
                for _ in range(curlen + len(values_multi) - self._maxsize_cached):
                    assert data_cursor.delete(data_cursor.key())
                    if self.get_metadata:
                        assert meta_cursor.delete(meta_cursor.key())

            _, added = data_cursor.putmulti(
                values_multi, append = True
            )
            assert added == len(values_multi)
            if meta_multi is not None:
                _, added = meta_cursor.putmulti(
                    meta_multi, append = True
                )
                assert added == len(meta_multi)

            if implicit:
                self._increment_version(cursors)
                txn.commit()
            else:
                changed.add(self)
        except BaseException as exc:
            self._abort(exc, txn, implicit)

    def __bool__(self) -> bool:
        return len(self) > 0

    __reversed__: Callable[..., Iterator[Any]] = Missing()

    __iter__: Callable[..., Iterator[Any]] = Missing()
