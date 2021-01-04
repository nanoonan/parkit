# pylint: disable = broad-except, no-value-for-parameter, non-parent-init-called, super-init-not-called
import logging
import math
import pickle
import queue
import struct

from typing import (
    Any, ByteString, Callable, cast, Optional, Tuple
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.object import ObjectMeta
from parkit.adapters.sized import Sized
from parkit.storage import (
    Entity,
    Missing,
    transaction
)
from parkit.utility import (
    compile_function,
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
        result = cursor.pop(cursor.key()) if {0} else None
        if txn:
            txn.commit()
    except BaseException as exc:
        self._Entity__abort(exc, txn)
    finally:
        if txn and cursor:
            cursor.close()
    if result is None:
        raise queue.Empty()
    return self.decitemval(result) if self.decitemval else result
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

    __slots__ = {'__maxsize'}

    decitemval: Callable[..., Any] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encitemval: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    def __init__(
        self,
        path: str,
        /, *,
        create: bool = True,
        bind: bool = True,
        maxsize: int = 0
    ) -> None:
        name, namespace = resolve_path(path)
        Entity.__init__(
            self, name,
            properties = [{'integerkey': True}],
            namespace = namespace,
            create = create, bind = bind,
            custom_descriptor = {'maxsize': maxsize if maxsize > 0 else None}
        )
        self.__maxsize = self.descriptor['custom']['maxsize']
        if not self.__maxsize:
            self.__maxsize = math.inf

    @property
    def maxsize(self) -> int:
        return self.__maxsize

    def __setstate__(self, from_wire):
        super().__setstate__(from_wire)
        self.__maxsize = self.descriptor['custom']['maxsize']
        if not self.__maxsize:
            self.__maxsize = math.inf

    def put(
        self,
        item: Any,
        block: bool = True,
        timeout: Optional[float] = None,
        polling_interval: Optional[float] = None
    ):
        if self.__maxsize != math.inf and block and (timeout is None or timeout > 0):
            try:
                for _ in polling_loop(
                    polling_interval if polling_interval is not None else \
                    constants.DEFAULT_ADAPTER_POLLING_INTERVAL,
                    timeout = timeout
                ):
                    if self.__len__() < self.__maxsize:
                        with transaction(self):
                            if self.__len__() < self.__maxsize:
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
    ) -> None:
        item = self.encitemval(item) if self.encitemval else item
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

            if self.__maxsize != math.inf and \
            txn.stat(self._Entity__userdb[0])['entries'] >= self.__maxsize:
                raise queue.Full()

            if not cursor.last():
                key = struct.pack('@N', 0)
            else:
                key = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)

            assert cursor.put(key = key, value = item, append = True)

            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)
        finally:
            if implicit and cursor:
                cursor.close()

    def get(
        self,
        block: bool = True,
        timeout: Optional[float] = None,
        polling_interval: Optional[float] = None
    ) -> Any:
        if not block or timeout is not None and timeout <= 0:
            return self.get_nowait()
        try:
            for _ in polling_loop(
                polling_interval if polling_interval is not None else \
                constants.DEFAULT_ADAPTER_POLLING_INTERVAL,
                timeout = timeout
            ):
                try:
                    return self.get_nowait()
                except queue.Empty:
                    pass
        except TimeoutError as exc:
            raise queue.Empty() from exc

    qsize: Callable[..., int] = Sized.__len__

    get_nowait: Callable[..., Any] = Missing()

    def empty(self) -> bool:
        return self.__len__() == 0

    def full(self) -> bool:
        return self.__len__() == self.__maxsize

class QueueMeta(ObjectMeta):

    def __initialize_class__(cls) -> None:
        if isinstance(cls.get_nowait, Missing):
            code, method =  mkgetnowait(fifo = True)
            setattr(cls, 'get_nowait', method)
            setattr(cls, 'get_nowaitcode', code)
        super().__initialize_class__()

class Queue(_Queue, metaclass = QueueMeta):
    pass

class LifoQueueMeta(ObjectMeta):

    def __initialize_class__(cls) -> None:
        if isinstance(cls.get_nowait, Missing):
            code, method =  mkgetnowait(fifo = False)
            setattr(cls, 'get_nowait', method)
            setattr(cls, 'get_nowaitcode', code)
        super().__initialize_class__()

class LifoQueue(_Queue, metaclass = LifoQueueMeta):
    pass

class BytesQueue(Queue):
    encitemval = None
    decitemval = None

class BytesLifoQueue(LifoQueue):
    encitemval = None
    decitemval = None