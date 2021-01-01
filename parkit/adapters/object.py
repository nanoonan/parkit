# pylint: disable = broad-except, using-constant-test
import logging
import pickle

from typing import (
    Any, ByteString, Callable, cast, Optional
)

import parkit.storage.threadlocal as thread

from parkit.exceptions import (
    abort,
    ObjectNotFoundError
)
from parkit.storage import Entity

logger = logging.getLogger(__name__)

class Object(Entity):

    encode_attrkey: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    encode_attrval: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    decode_attrval: Callable[..., Any] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    def _getattr(
        self,
        key: Any
    ) -> Any:
        if key is not None:
            key = self.encode_attrkey(key) if self.encode_attrkey else key
            key = b''.join([self._uuid_bytes, key])
        else:
            key = self._uuid_bytes
        try:
            implicit = False
            cursor = None
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._attribute_db)
            else:
                cursor = thread.local.cursors[id(self._attribute_db)]
            obj_uuid = txn.get(key = self._encoded_name, db = self._name_db)
            if obj_uuid == self._uuid_bytes:
                if cursor.set_key(key):
                    result = cursor.value()
                else:
                    result = None
                if implicit:
                    txn.commit()
            else:
                raise ObjectNotFoundError()
        except BaseException as exc:
            if implicit and txn:
                txn.abort()
            abort(exc)
        finally:
            if implicit and cursor:
                cursor.close()
        return self.decode_attrval(result) if result is not None and self.decode_attrval else result

    def _delattr(
        self,
        key: Any
    ) -> None:
        if key is not None:
            key = self.encode_attrkey(key) if self.encode_attrkey else key
            key = b''.join([self._uuid_bytes, key])
        else:
            key = self._uuid_bytes
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            obj_uuid = txn.get(key = self._encoded_name, db = self._name_db)
            if obj_uuid == self._uuid_bytes:
                result = txn.delete(key = key, db = self._attribute_db)
                if implicit:
                    if result and self._versioned:
                        self.increment_version(use_transaction = txn)
                    txn.commit()
                elif self._versioned:
                    thread.local.changed.add(self)
            else:
                raise ObjectNotFoundError()
        except BaseException as exc:
            if implicit and txn:
                txn.abort()
            abort(exc)

    def _putattr(
        self,
        key: Any,
        value: Any
    ):
        if key is not None:
            key = self.encode_attrkey(key) if self.encode_attrkey else key
            key = b''.join([self._uuid_bytes, key])
        else:
            key = self._uuid_bytes
        value = self.encode_attrval(value) if self.encode_attrval else value
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            obj_uuid = txn.get(key = self._encoded_name, db = self._name_db)
            if obj_uuid == self._uuid_bytes:
                assert txn.put(
                    key = key, value = value, overwrite = True, append = False,
                    db = self._attribute_db
                )
                if implicit:
                    if self._versioned:
                        self.increment_version(use_transaction = txn)
                    txn.commit()
                elif self._versioned:
                    thread.local.changed.add(self)
            else:
                raise ObjectNotFoundError()
        except BaseException as exc:
            if implicit and txn:
                txn.abort()
            abort(exc)

class Attr:

    def __init__(
        self, readonly: bool = False
    ) -> None:
        self._readonly: bool = readonly
        self.name: Optional[str] = None

    def __set_name__(
        self,
        owner: Object,
        name: str
    ) -> None:
        self.name = name

    def __get__(
        self,
        obj: Object,
        objtype: type = None
    ) -> Any:
        return obj._getattr(self.name)

    def __set__(
        self,
        obj: Object,
        value: Any
    ) -> None:
        if self._readonly:
            raise AttributeError()
        obj._putattr(self.name, value)
