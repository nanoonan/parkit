# pylint: disable = not-callable, broad-except, attribute-defined-outside-init, no-self-use
import logging
import math
import pickle
import queue
import struct

from typing import (
    Any, ByteString, Callable, cast, Dict, Optional, Tuple
)

import parkit.storage.threadlocal as thread

from parkit.adapters.sized import Sized
from parkit.storage.entitymeta import (
    ClassBuilder,
    Missing
)
from parkit.utility import compile_function

logger = logging.getLogger(__name__)

def mkget(fifo: bool = True) -> Callable[..., Any]:
    code = """
def method(self, metadata):
    try:
        txn, cursors, changed, implicit = \
        thread.local.context.get(self._Entity__env, write = not metadata)

        cursor = cursors[self._Entity__userdb[0]]

        data = meta = None
        if {0}:
            key = cursor.key()
            if metadata:
                meta = txn.get(key = key, db = self._Entity__userdb[1]) if self.get_metadata else False
            else:
                data = cursor.pop(key = key)
                if self.get_metadata:
                    meta = txn.pop(key = key, db = self._Entity__userdb[1])
        if implicit:
            if not metadata and data is not None:
                self._Entity__increment_version(cursors)
            txn.commit()
        elif not metadata and data is not None:
            changed.add(self)
    except BaseException as exc:
        self._Entity__abort(exc, txn, implicit)
    finally:
        if implicit and cursor:
            cursor.close()
    if metadata:
        if meta is None:
            raise queue.Empty()
        else:
            return pickle.loads(meta) if self.get_metadata else None
    elif data is None:
        raise queue.Empty()
    return (self.decode_value(data, pickle.loads(meta)) if self.get_metadata else self.decode_value(data)) \
    if self.decode_value else data
"""
    insert = """
    cursor.first()
    """.strip() if fifo else """
    cursor.last()
    """.strip()
    return compile_function(
        code.format(insert), glbs = globals(), defaults = (False,)
    )

class QueueBase(Sized):

    __maxsize_cached = math.inf

    get_metadata: Optional[Callable[..., Any]] = None

    decode_value: Optional[Callable[..., Any]] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encode_value: Optional[Callable[..., ByteString]] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        create: bool = True,
        bind: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
        maxsize: int = 0,
        site: Optional[str] = None,
        on_init: Optional[Callable[[bool], None]] = None
    ):
        def _on_init(create: bool):
            if create:
                self.__maxsize = maxsize if maxsize > 0 else math.inf
            if on_init:
                on_init(create)

        super().__init__(
            path,
            db_properties = [{'integerkey': True}, {'integerkey': True}],
            create = create, bind = bind, on_init = _on_init,
            metadata = metadata, site = site
        )

        self.__maxsize_cached = self.__maxsize

    def __setstate__(self, from_wire: Tuple[str, str, str]):
        super().__setstate__(from_wire)
        self.__maxsize_cached = self.__maxsize

    @property
    def maxsize(self) -> Optional[int]:
        return int(self.__maxsize_cached) if self.__maxsize_cached != math.inf else None

    def put(
        self,
        item: Any,
        /
    ):
        meta = pickle.dumps(self.get_metadata(item)) if self.get_metadata else None
        item_bytes = self.encode_value(item) if self.encode_value else item
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True)

            cursor = cursors[self._Entity__userdb[0]]

            if self.__maxsize_cached != math.inf and \
            txn.stat(self._Entity__userdb[0])['entries'] >= self.__maxsize_cached:
                raise queue.Full()

            if not cursor.last():
                key = struct.pack('@N', 0)
            else:
                key = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)

            assert cursor.put(key = key, value = item_bytes, append = True)
            if self.get_metadata:
                assert txn.put(
                    key = key, value = meta, append = True, db = self._Entity__userdb[1]
                )
            if implicit:
                self._Entity__increment_version(cursors)
                txn.commit()
            else:
                changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        finally:
            if implicit and cursor:
                cursor.close()

    qsize: Callable[..., int] = Sized.__len__

    get: Callable[..., Any] = Missing()

    def empty(self) -> bool:
        return self.__len__() == 0

    def full(self) -> bool:
        return self.__len__() == self.__maxsize_cached

class QueueMeta(ClassBuilder):

    def __build_class__(cls, target, attr):
        if target == Queue and attr == 'get':
            setattr(target, 'get', mkget(fifo = True))

class Queue(QueueBase, metaclass = QueueMeta):
    pass

class LifoQueueMeta(ClassBuilder):

    def __build_class__(cls, target, attr):
        if isinstance(target, LifoQueue) and attr == 'get':
            setattr(target, 'get', mkget(fifo = False))

class LifoQueue(QueueBase, metaclass = LifoQueueMeta):
    pass
