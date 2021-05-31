# pylint: disable = broad-except, no-value-for-parameter, non-parent-init-called, super-init-not-called
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
from parkit.storage import (
    Entity,
    EntityMeta,
    Missing,
    transaction
)
from parkit.utility import (
    compile_function,
    getenv,
    polling_loop,
    resolve_path
)

logger = logging.getLogger(__name__)

def mkgetnowait(fifo: bool = True) -> Tuple[str, Callable[..., Any]]:
    code = """
def method(self):
    try:
        txn = cursor = None
        cursor = thread.local.cursors[id(self._Entity__userdb[0])]
        if not cursor:
            txn = self._Entity__env.begin(write = True)
            cursor = txn.cursor(db = self._Entity__userdb[0])
        if {0}:
            key = cursor.key()
            data = cursor.pop(key)
            meta = txn.pop(key, db = self._Entity__userdb[1]) if self.get_metadata else None
        else:
            data = meta = None
        if txn:
            txn.commit()
    except BaseException as exc:
        self._Entity__abort(exc, txn)
    finally:
        if txn and cursor:
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
        create: bool = True,
        bind: bool = True,
        type_check: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
        maxsize: int = 0
    ):
        if path is not None:
            name, namespace = resolve_path(path)
        else:
            name = namespace = None
        Entity.__init__(
            self, name,
            properties = [{'integerkey': True}, {'integerkey': True}],
            namespace = namespace,
            create = create, bind = bind,
            type_check = type_check,
            metadata = metadata
        )
        if '_maxsize' not in self.attributes():
            self._maxsize = maxsize if maxsize > 0 else math.inf
        self._maxsize_cached = self._maxsize

    def __setstate__(self, from_wire: Tuple[str, str, str, str]):
        super().__setstate__(from_wire)
        self._maxsize_cached = self._maxsize

    @property
    def maxsize(self) -> Optional[int]:
        return int(self._maxsize_cached) if self._maxsize_cached != math.inf else None

    def put(
        self,
        item: Any,
        /, *,
        block: bool = True,
        timeout: Optional[float] = None,
        polling_interval: Optional[float] = None
    ):
        default_polling_interval = getenv(constants.ADAPTER_POLLING_INTERVAL_ENVNAME, float)
        if self._maxsize_cached != math.inf and block and (timeout is None or timeout > 0):
            try:
                for _ in polling_loop(
                    polling_interval if polling_interval is not None else \
                    default_polling_interval,
                    timeout = timeout
                ):
                    if self.__len__() < self._maxsize_cached:
                        with transaction(self.namespace):
                            if self.__len__() < self._maxsize_cached:
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
        item = self.encode_value(item) if self.encode_value else item
        try:
            implicit = False
            cursor = None
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin(write = True)
                cursor = txn.cursor(db = self._Entity__userdb[0])
            else:
                cursor = thread.local.cursors[id(self._Entity__userdb[0])]

            if self._maxsize_cached != math.inf and \
            txn.stat(self._Entity__userdb[0])['entries'] >= self._maxsize_cached:
                raise queue.Full()

            if not cursor.last():
                key = struct.pack('@N', 0)
            else:
                key = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)

            assert cursor.put(key = key, value = item, append = True)
            if self.get_metadata:
                assert txn.put(
                    key = key, value = meta, append = True, db = self._Entity__userdb[1]
                )
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)
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
        return self.__len__() == self._maxsize_cached

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
