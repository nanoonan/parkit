# pylint: disable = broad-except, protected-access
import logging
import struct

from typing import (
    Any, ByteString, Callable, Optional
)

import parkit.storage.threadlocal as thread

from parkit.exceptions import abort

logger = logging.getLogger(__name__)

def get(
    db0: int,
    decode_value: Callable[..., Any],
    fifo: bool = True
) -> Callable[..., Any]:

    def _get(
        self,
        fifo: bool = fifo,
        decode_value: Optional[Callable[..., Any]] = decode_value
    ) -> Any:
        try:
            txn = None
            cursor = thread.local.cursors[id(self._user_db[db0])]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[db0])
            exists = cursor.first() if fifo else cursor.last()
            if exists:
                result = cursor.pop(cursor.key())
            else:
                result = None
            if txn:
                if exists and self._versioned:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif exists and self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()
        return decode_value(result) if exists and decode_value else result

    return _get

def put(
    db0: int,
    encode_value: Callable[..., ByteString]
) -> Callable[..., None]:

    def _put(
        self,
        value: Any,
        encode_value: Optional[Callable[..., ByteString]] = encode_value
    ) -> None:
        value = encode_value(value) if encode_value else value
        try:
            txn = None
            cursor = thread.local.cursors[id(self._user_db[db0])]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[db0])
            if not cursor.last():
                key = struct.pack('@N', 0)
            else:
                key = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)
            result = cursor.put(key = key, value = value, append = True)
            if txn:
                if result and self._versioned:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif result and self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()

    return _put
