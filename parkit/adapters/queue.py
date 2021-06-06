# pylint: disable = not-callable, broad-except
import logging
import math
import pickle
import queue
import struct

from typing import (
    Any, ByteString, Callable, cast, Dict, Optional, Tuple
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.sized import Sized
from parkit.storage.context import transaction_context
from parkit.storage.entitymeta import EntityMeta
from parkit.storage.missing import Missing
from parkit.utility import (
    compile_function,
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

def mkgetnowait(fifo: bool = True) -> Tuple[str, Callable[..., Any]]:
    code = """
def method(self):
    try:
        txn, cursors, _, implicit = \
        thread.local.context.get(self._Entity__env, write = True)
        cursor = cursors[self._Entity__userdb[0]]
        if {0}:
            key = cursor.key()
            data = cursor.pop(key)
            meta = txn.pop(key, db = self._Entity__userdb[1]) if self.get_metadata else None
        else:
            data = meta = None
        if implicit:
            txn.commit()
    except BaseException as exc:
        self._Entity__abort(exc, txn, implicit)
    finally:
        if implicit and cursor:
            cursor.close()
    if data is None:
        raise queue.Empty()
    return (self.decode_value(data, pickle.loads(meta)) if self.get_metadata else self.decode_value(data)) \
    if self.decode_value else data
"""
    insert = """
    cursor.first()
    """.strip() if fifo else """
    cursor.last()
    """.strip()
    return (code.format(insert), compile_function(
        code, insert, glbs = globals()
    ))

class _Queue(Sized):

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
        super().__init__(
            path,
            db_properties = [{'integerkey': True}, {'integerkey': True}],
            create = create, bind = bind, on_init = on_init,
            metadata = metadata, site = site
        )
        if '__maxsize' not in self.attributes():
            self.__maxsize = maxsize if maxsize > 0 else math.inf
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
        /, *,
        block: bool = True,
        timeout: Optional[float] = None,
        polling_interval: Optional[float] = None
    ):
        if self.__maxsize_cached != math.inf and block and (timeout is None or timeout > 0):
            try:
                for _ in polling_loop(
                    polling_interval if polling_interval is not None else \
                    getenv(constants.ADAPTER_POLLING_INTERVAL_ENVNAME, float),
                    timeout = timeout
                ):
                    if self.__len__() < self.__maxsize_cached:
                        with transaction_context(self._Entity__env, write = True):
                            if self.__len__() < self.__maxsize_cached:
                                self.put_nowait(item)
                                break
            except TimeoutError as exc:
                raise queue.Full() from exc
        else:
            self.put_nowait(item)

    def put_nowait(
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

    def get(
        self,
        /, *,
        block: bool = True,
        timeout: Optional[float] = None,
        polling_interval: Optional[float] = None
    ) -> Any:
        if not block or timeout is not None and timeout <= 0:
            return self.get_nowait()
        try:
            default_polling_interval = getenv(constants.ADAPTER_POLLING_INTERVAL_ENVNAME, float)
            for _ in polling_loop(
                polling_interval if polling_interval is not None else \
                default_polling_interval,
                timeout = timeout
            ):
                try:
                    return self.get_nowait()
                except queue.Empty:
                    pass
        except TimeoutError as exc:
            raise queue.Empty() from exc
        return None

    qsize: Callable[..., int] = Sized.__len__

    get_nowait: Callable[..., Any] = Missing()

    def empty(self) -> bool:
        return self.__len__() == 0

    def full(self) -> bool:
        return self.__len__() == self.__maxsize_cached

class QueueMeta(EntityMeta):

    def __initialize_class__(cls):
        if isinstance(cast(_Queue, cls).get_nowait, Missing):
            code, method =  mkgetnowait(fifo = True)
            setattr(cls, 'get_nowait', method)
            setattr(cls, 'get_nowaitcode', code)
        super().__initialize_class__()

class Queue(_Queue, metaclass = QueueMeta):
    pass

class LifoQueueMeta(EntityMeta):

    def __initialize_class__(cls):
        if isinstance(cast(_Queue, cls).get_nowait, Missing):
            code, method =  mkgetnowait(fifo = False)
            setattr(cls, 'get_nowait', method)
            setattr(cls, 'get_nowaitcode', code)
        super().__initialize_class__()

class LifoQueue(_Queue, metaclass = LifoQueueMeta):
    pass
