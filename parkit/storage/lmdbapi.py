# pylint: disable = broad-except, c-extension-no-member
import logging
import struct
import uuid

from typing import Optional

import lmdb
import orjson

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.exceptions import (
    abort,
    ObjectNotFoundError
)
from parkit.storage.lmdbstate import LMDBState
from parkit.storage.lmdbtypes import Descriptor

logger = logging.getLogger(__name__)

class LMDBAPI(LMDBState):

    @property
    def path(self) -> str:
        return '/'.join([
            self._namespace,
            self._encoded_name.decode('utf-8')
        ])

    @property
    def uuid(self) -> str:
        return str(uuid.UUID(bytes = self._uuid_bytes))

    @property
    def versioned(self) -> bool:
        return self._versioned

    @property
    def persistent(self) -> bool:
        return self.path.startswith(''.join([constants.PERSISTENT_NAMESPACE, '/']))

    @property
    def creator(self) -> bool:
        return self._creator

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def name(self) -> str:
        return self._encoded_name.decode('utf-8')

    def increment_version(self, use_transaction: Optional[lmdb.Transaction] = None) -> None:
        if not self._versioned:
            return
        try:
            txn = None
            if use_transaction:
                txn = use_transaction
                cursor = txn.cursor(db = self._version_db)
            else:
                cursor = thread.local.cursors[self._version_dbuid]
                if not cursor:
                    txn = self._environment.begin()
                    cursor = txn.cursor(db = self._version_db)
            if cursor.set_key(self._uuid_bytes):
                version = struct.pack('@N', struct.unpack('@N', cursor.value())[0] + 1)
                assert cursor.put(key = self._uuid_bytes, value = version)
            else:
                raise ObjectNotFoundError()
            if txn and not use_transaction:
                txn.commit()
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()

    @property
    def exists(self) -> bool:
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin()
            obj_uuid = txn.get(key = self._encoded_name, db = self._name_db)
            result = obj_uuid == self._uuid_bytes
            if implicit:
                txn.commit()
        except BaseException as exc:
            if txn and implicit:
                txn.abort()
            abort(exc)
        return result

    @property
    def version(self) -> int:
        try:
            txn = None
            cursor = thread.local.cursors[self._version_dbuid]
            if not cursor:
                txn = self._environment.begin()
                cursor = txn.cursor(db = self._version_db)
            if cursor.set_key(self._uuid_bytes):
                version = cursor.value()
                version = struct.unpack('@N', version)[0]
            else:
                raise ObjectNotFoundError()
            if txn:
                txn.commit()
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()
        return version

    def drop(self) -> None:
        try:
            txn = None
            cursor = thread.local.cursors[self._metadata_dbuid]
            implicit = False
            if not cursor:
                implicit = True
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._metadata_db)
            else:
                txn = thread.local.transaction
            obj_uuid = txn.get(key = self._encoded_name, db = self._name_db)
            if obj_uuid == self._uuid_bytes:
                for database in self._user_db:
                    txn.drop(database, delete = True)
                assert txn.delete(key = self._encoded_name, db = self._name_db)
                assert txn.delete(key = self._uuid_bytes, db = self._version_db)
                assert txn.delete(key = self._uuid_bytes, db = self._descriptor_db)
                if cursor.set_range(self._uuid_bytes):
                    if cursor.key().startswith(self._uuid_bytes):
                        while True:
                            if not cursor.delete():
                                break
                            if not cursor.key().startswith(self._uuid_bytes):
                                break
            if implicit:
                txn.commit()
        except BaseException as exc:
            if txn and implicit:
                txn.abort()
            abort(exc)
        finally:
            if implicit and cursor:
                cursor.close()

    @property
    def descriptor(self) -> Descriptor:
        try:
            txn = None
            cursor = thread.local.cursors[self._descriptor_dbuid]
            if not cursor:
                txn = self._environment.begin()
                cursor = txn.cursor(db = self._descriptor_db)
            if cursor.set_key(self._uuid_bytes):
                result = cursor.value()
                result = bytes(result) if isinstance(result, memoryview) else result
            else:
                raise ObjectNotFoundError()
            if txn:
                txn.commit()
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()
        return orjson.loads(result)
