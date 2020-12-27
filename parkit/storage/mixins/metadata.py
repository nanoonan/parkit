# pylint: disable = broad-except, protected-access
import logging

from typing import (
    Any, ByteString, Callable, Optional
)

import parkit.storage.threadlocal as thread

from parkit.exceptions import (
    abort,
    ObjectNotFoundError
)
from parkit.storage.lmdbapi import LMDBAPI

logger = logging.getLogger(__name__)

def get(
    encode_key: Callable[..., ByteString],
    decode_metadata: Callable[..., Any]
) -> Callable[..., Any]:

    def _get(
        self: LMDBAPI,
        key: Any = None,
        encode_key: Optional[Callable[..., ByteString]] = encode_key,
        decode_metadata: Optional[Callable[..., Any]] = decode_metadata
    ) -> Any:
        try:
            if key is not None:
                key = key if not encode_key else encode_key(key)
                key = b''.join([self._uuid_bytes, key])
            else:
                key = self._uuid_bytes
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._metadata_db)
            else:
                cursor = thread.local.cursors[self._metadata_dbuid]
            obj_uuid = txn.get(key = self._encoded_name, db = self._name_db)
            if obj_uuid == self._uuid_bytes:
                if cursor.set_key(key):
                    result = cursor.value()
                else:
                    result = None
                if txn and implicit:
                    txn.commit()
            else:
                raise ObjectNotFoundError()
        except BaseException as exc:
            if txn and implicit:
                txn.abort()
            abort(exc)
        finally:
            if implicit and cursor:
                cursor.close()
        return decode_metadata(result) if result is not None and decode_metadata else result

    return _get

def delete(encode_key: Callable[..., ByteString]) -> Callable[..., None]:

    def _delete(
        self: LMDBAPI,
        key: Any = None,
        encode_key: Optional[Callable[..., ByteString]] = encode_key
    ) -> None:
        try:
            if key is not None:
                key = key if not encode_key else encode_key(key)
                key = b''.join([self._uuid_bytes, key])
            else:
                key = self._uuid_bytes
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            obj_uuid = txn.get(key = self._encoded_name, db = self._name_db)
            if obj_uuid == self._uuid_bytes:
                txn.delete(key = key, db = self._metadata_db)
                if implicit:
                    txn.commit()
            else:
                raise ObjectNotFoundError()
        except BaseException as exc:
            if txn and implicit:
                txn.abort()
            abort(exc)

    return _delete

def put(
    encode_key: Callable[..., ByteString],
    encode_metadata: Callable[..., ByteString]
) -> Callable[..., None]:

    def _put(
        self: LMDBAPI,
        metadata: Any,
        key: Any = None,
        encode_key: Optional[Callable[..., ByteString]] = encode_key,
        encode_metadata: Optional[Callable[..., ByteString]] = encode_metadata
    ):
        try:
            if key is not None:
                key = key if not encode_key else encode_key(key)
                key = b''.join([self._uuid_bytes, key])
            else:
                key = self._uuid_bytes
            metadata = metadata if not encode_metadata else encode_metadata(metadata)
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            obj_uuid = txn.get(key = self._encoded_name, db = self._name_db)
            if obj_uuid == self._uuid_bytes:
                assert txn.put(
                    key = key, value = metadata, overwrite = True, append = False,
                    db = self._metadata_db
                )
                if implicit:
                    txn.commit()
            else:
                raise ObjectNotFoundError()
        except BaseException as exc:
            if txn and implicit:
                txn.abort()
            abort(exc)

    return _put
