# pylint: disable = raise-missing-from, abstract-method, not-callable, broad-except, no-self-use
import logging
import pickle
import queue
import struct

from typing import (
    Any, Callable, Iterator
)

import parkit.storage.threadlocal as thread

from parkit.adapters.array import Array
from parkit.storage.entitymeta import Missing

logger = logging.getLogger(__name__)

class QueueBase(Array):

    __setitem__: Callable[..., None] = Missing()

    __getitem__: Callable[..., Any] = Missing()

    __reversed__: Callable[..., Iterator[Any]] = Missing()

    __iter__: Callable[..., Iterator[Any]] = Missing()

    pop: Callable[..., Any] = Missing()

    popleft: Callable[..., Any] = Missing()

    append: Callable[..., None] = Missing()

    extend: Callable[..., None] = Missing()

    def _iterator(self) -> Iterator[Any]:
        return Array.__iter__(self)

    def put(
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
            if curlen == self._maxsize_cached:
                raise queue.Full()

            cursor = cursors[self._userdb[0]]

            if not cursor.last():
                key_bytes = struct.pack('@N', 0)
            else:
                key_bytes = struct.pack(
                    '@N',
                    struct.unpack('@N', cursor.key())[0] + 1
                )
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

    def qsize(self):
        return len(self)

    def empty(self) -> bool:
        return len(self) == 0

    def full(self) -> bool:
        return len(self) == self._maxsize_cached

class Queue(QueueBase):

    def get(self) -> Any:
        try:
            return Array.popleft(self)
        except IndexError:
            raise queue.Empty()

class LifoQueue(QueueBase):

    def get(self) -> Any:
        try:
            return Array.pop(self)
        except IndexError:
            raise queue.Empty()
