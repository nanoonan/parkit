# pylint: disable = no-value-for-parameter
import logging
import pickle
import queue
import struct

from typing import (
    Any, ByteString, Callable, cast
)

import parkit.storage.threadlocal as thread

from parkit.storage import (
    Missing,
    EntityMeta
)
from parkit.adapters.sized import Sized
from parkit.exceptions import abort
from parkit.utility import (
    compile_function,
    resolve_path
)
logger = logging.getLogger(__name__)

def mkgetnowait(left: bool = True) -> Callable[..., Any]:
    code = """
def method(self) -> Any:
    try:
        implicit = False
        cursor = None
        txn = thread.local.transaction
        if not txn:
            implicit = True
            txn = self._environment.begin(write = True)
        {0}
        cursor = txn.cursor(db = database) if implicit else thread.local.cursors[id(database)]

        result = cursor.pop(cursor.key()) if \
        (cursor.last() if last else cursor.first()) else None

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
        raise queue.Empty()
    return self.decode_item(result) if self.decode_item else result
"""
    insert = """
    last, database = (True, self._user_db[0]) if txn.stat(self._user_db[0])['entries'] \
    else (False, self._user_db[1])
    """.strip() if left else """
    last, database = (True, self._user_db[1]) \
    if txn.stat(self._user_db[1])['entries'] else (False, self._user_db[0])
    """.strip()
    return compile_function(
        code, insert,
        globals_dict = dict(
            struct = struct, BaseException = BaseException, thread = thread,
            abort = abort, id = id, queue = queue
        )
    )

# FIXME: check max size

def mkputnowait(left: bool = False) -> Callable[..., None]:
    code = """
def method(
    self,
    item: Any
) -> None:
    item = self.encode_item(item) if self.encode_item else item
    try:
        txn = cursor = None
        database = {0}
        cursor = thread.local.cursors[id(database)]
        if not cursor:
            txn = self._environment.begin(write = True)
            cursor = txn.cursor(db = database)
        if not cursor.last():
            key = struct.pack('@N', 0)
        else:
            key = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)
        assert cursor.put(key = key, value = item, append = True)
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
    return compile_function(
        code, 'self._user_db[0]' if left else 'self._user_db[1]',
        globals_dict = dict(
            struct = struct, BaseException = BaseException, thread = thread,
            abort = abort, id = id
        )
    )

class DequeMeta(EntityMeta):

    def __initialize_class__(cls):
        super().__initialize_class__()
        if isinstance(cls.pop, Missing):
            setattr(cls, 'get_nowait', mkgetnowait(left = False))
        if isinstance(cls.popleft, Missing):
            setattr(cls, 'getleft_nowait', mkgetnowait(left = True))
        if isinstance(cls.append, Missing):
            setattr(cls, 'put_nowait', mkputnowait(left = False))
        if isinstance(cls.appendleft, Missing):
            setattr(cls, 'putleft_nowait', mkputnowait(left = True))

    def __call__(cls, *args, **kwargs):
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)

class Deque(Sized, metaclass = DequeMeta):

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
            name, properties = [{'integerkey':True}, {'integerkey':True}],
            namespace = namespace, create = create, bind = bind,
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

    def empty(self) -> bool:
        return self.__len__() == 0

    def full(self) -> bool:
        return self.__len__() < self._maxsize

    qsize = Sized.__len__

    get_nowait: Callable[..., Any] = Missing()

    getleft_nowait: Callable[..., Any] = Missing()

    put_nowait: Callable[..., None] = Missing()

    putleft_nowait: Callable[..., None] = Missing()
