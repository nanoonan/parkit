# pylint: disable = broad-except, protected-access
import logging
import struct

from typing import (
    Any, ByteString, Callable, Optional
)

import parkit.storage.threadlocal as thread

from parkit.exceptions import abort

logger = logging.getLogger(__name__)

def pop(
    db0: int, db1: int, left: bool,
    decode_value: Callable[..., Any]
) -> Callable[..., Any]:

    def _pop(
        self,
        decode_value: Optional[Callable[..., Any]] = decode_value
    ) -> Any:
        try:
            implicit = False
            cursor = None
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            if left:
                last, database = (True, self._user_db[db0]) \
                if txn.stat(self._user_db[db0])['entries'] else (False, self._user_db[db1])
            else:
                last, database = (True, self._user_db[db1]) \
                if txn.stat(self._user_db[db1])['entries'] else (False, self._user_db[db0])

            cursor = txn.cursor(db = database) if implicit else thread.local.cursors[id(database)]

            result = cursor.pop(cursor.key()) if \
            (cursor.last() if last else cursor.first()) else None

            if implicit:
                txn.commit()
        except BaseException as exc:
            if implicit:
                txn.abort()
            abort(exc)
        finally:
            if implicit and cursor:
                cursor.close()
        return decode_value(result) if result is not None and decode_value else result

    return _pop

def append(
    db0: int, db1: int, left: bool,
    encode_value: Callable[..., ByteString]
) -> Callable[..., None]:

    def _append(
        self,
        value: Any,
        encode_value: Optional[Callable[..., ByteString]] = encode_value,
    ) -> None:
        value = encode_value(value) if encode_value else value
        try:
            txn = cursor = None
            database = self._user_db[db0] if left else self._user_db[db1]
            cursor = thread.local.cursors[id(database)]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = database)
            if not cursor.last():
                key = struct.pack('@N', 0)
            else:
                key = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)
            assert cursor.put(key = key, value = value, append = True)
            if txn:
                txn.commit()
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()

    return _append
