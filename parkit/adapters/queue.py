# pylint: disable = broad-except, using-constant-test
import logging
import math
import pickle
import queue
import struct

from typing import (
    Any, ByteString, Callable, cast, Optional
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.sized import Sized
from parkit.exceptions import (
    abort,
    TimeoutError
)
from parkit.storage import (
    EntityMeta,
    Missing,
    transaction
)
from parkit.utility import (
    compile_function,
    polling_loop,
    resolve_path
)

logger = logging.getLogger(__name__)

def mkgetnowait(fifo: bool = True) -> Callable[..., Any]:
    code = """
def method(self):
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._user_db[0])]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[0])
            result = cursor.pop(cursor.key()) if {0} else None
            if txn:
                txn.commit()
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()
        if result is None:
            raise queue.Empty()
        return self.decode_item(result) if self.decode_item else result
"""
    insert = """
    cursor.first()
    """.strip() if fifo else """
    cursor.last()
    """.strip()
    return compile_function(
        code, insert,
        globals_dict = dict(
            queue = queue, BaseException = BaseException, thread = thread,
            abort = abort, id = id
        )
    )

class _Queue(Sized):

    decode_item: Callable[..., Any] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encode_item: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    def __init__(
        self,
        path: str,
        create: bool = True,
        bind: bool = True,
        maxsize: int = 0
    ) -> None:
        name, namespace = resolve_path(path)
        super().__init__(
            name,
            properties = [{'integerkey': True}],
            namespace = namespace,
            create = create, bind = bind,
            custom_descriptor = {'maxsize': maxsize if maxsize > 0 else math.inf}
        )
        self._maxsize = self.descriptor['custom']['maxsize']
        if self._maxsize is None:
            self._maxsize = math.inf

    @property
    def maxsize(self) -> int:
        return self._maxsize

    def __setstate__(self, from_wire):
        super().__setstate__(from_wire)
        self._maxsize = self.descriptor['custom']['maxsize']
        if self._maxsize is None:
            self._maxsize = math.inf

    def put(
        self,
        item: Any,
        block: bool = True,
        timeout: Optional[float] = None,
        polling_interval: Optional[float] = None
    ):
        if self._maxsize != math.inf and block and (timeout is None or timeout > 0):
            try:
                for _ in polling_loop(
                    polling_interval if polling_interval is not None else \
                    constants.DEFAULT_ADAPTER_POLLING_INTERVAL,
                    timeout = timeout
                ):
                    if self.__len__() < self._maxsize:
                        with transaction(self):
                            if self.__len__() < self._maxsize:
                                self.put_nowait(item)
                                break
            except TimeoutError as exc:
                raise queue.Full() from exc
        else:
            self.put_nowait(item)

    def put_nowait(
        self,
        item: Any
    ) -> None:
        item = self.encode_item(item) if self.encode_item else item
        try:
            implicit = False
            cursor = None
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[0])
            else:
                cursor = thread.local.cursors[id(self._user_db[0])]

            if self._maxsize != math.inf and \
            txn.stat(self._user_db[0])['entries'] >= self._maxsize:
                raise queue.Full()

            if not cursor.last():
                key = struct.pack('@N', 0)
            else:
                key = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)
            assert cursor.put(key = key, value = item, append = False)

            if implicit:
                txn.commit()
        except BaseException as exc:
            if implicit and txn:
                txn.abort()
            abort(exc)
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
        else:
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
        return self.__len__() < self._maxsize

class QueueMeta(EntityMeta):

    def __initialize_class__(cls):
        super().__initialize_class__()
        if isinstance(cls.get_nowait, Missing):
            setattr(cls, 'get_nowait', mkgetnowait(fifo = True))

    def __call__(cls, *args, **kwargs):
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)

class Queue(_Queue, metaclass = QueueMeta):
    pass

class LifoQueueMeta(EntityMeta):

    def __initialize_class__(cls):
        super().__initialize_class__()
        if isinstance(cls.get_nowait, Missing):
            setattr(cls, 'get_nowait', mkgetnowait(fifo = False))

    def __call__(cls, *args, **kwargs):
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)

class LifoQueue(_Queue, metaclass = LifoQueueMeta):
    pass