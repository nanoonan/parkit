# pylint: disable = c-extension-no-member, too-many-arguments, too-many-instance-attributes
import datetime
import logging
import struct
import uuid

from typing import (
    Any, Callable, List, Optional, Tuple
)

import orjson

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.storage.context import context
from parkit.exceptions import (
    ObjectExistsError,
    ObjectNotFoundError
)
from parkit.storage.lmdbapi import LMDBAPI
from parkit.storage.lmdbenv import (
    get_database,
    open_database,
    get_environment
)
from parkit.storage.lmdbstate import LMDBState
from parkit.storage.lmdbtypes import (
    Descriptor,
    LMDBProperties
)
from parkit.utility import (
    create_string_digest,
    get_qualified_class_name
)

logger = logging.getLogger(__name__)

class LMDBObject(LMDBAPI):

    def __init__(
        self,
        name: str,
        properties: List[LMDBProperties],
        namespace: Optional[str] = None,
        create: bool = True,
        bind: bool = True,
        versioned: bool = False,
        on_create: Callable[[], None] = lambda: None
    ) -> None:
        super().__init__()

        if not create and not bind:
            raise ValueError()

        self._encoded_name = name.encode('utf-8')
        self._namespace = namespace if namespace else constants.DEFAULT_NAMESPACE

        self._environment, self._name_db, self._metadata_db, self._version_db, \
        self._descriptor_db = get_environment(self._namespace)

        self._name_dbuid = id(self._name_db)
        self._metadata_dbuid = id(self._metadata_db)
        self._version_dbuid = id(self._version_db)
        self._descriptor_dbuid = id(self._descriptor_db)
        self._user_db = []
        self._user_dbuid = []

        if bind:
            descriptor = self._try_bind_lmdb()

        if descriptor:
            self._finish_bind_lmdb(descriptor)
            self._bind()
        elif create:
            with context(self._environment, write = True, inherit = True, buffers = False):
                self._create_lmdb(properties, versioned)
                self._bind()
                on_create()
        else:
            raise ObjectNotFoundError()

    def __hash__(self) -> int:
        return int(self._uuid_bytes)

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __eq__(self, other: Any) -> bool:
        if issubclass(type(other), LMDBState):
            return self._uuid_bytes == other._uuid_bytes
        return False

    def __getstate__(self) -> Tuple[str, bytes, bytes]:
        return (self._namespace, self._encoded_name, self._uuid_bytes)

    def __setstate__(self, from_wire: Tuple[str, bytes, bytes]) -> None:
        self._namespace, self._encoded_name, self._uuid_bytes = from_wire

        self._environment, self._name_db, self._metadata_db, self._version_db, \
        self._descriptor_db = get_environment(self._namespace)

        self._name_dbuid = id(self._name_db)
        self._metadata_dbuid = id(self._metadata_db)
        self._version_dbuid = id(self._version_db)
        self._descriptor_dbuid = id(self._descriptor_db)
        self._user_db = []
        self._user_dbuid = []
        with context(self._environment, write = False, inherit = True, buffers = False):
            txn = thread.local.transaction
            obj_uuid = txn.get(key = self._encoded_name, db = self._name_db)
            if obj_uuid != self._uuid_bytes:
                raise ObjectNotFoundError()
            result = txn.get(key = obj_uuid, db = self._descriptor_db)
            result = bytes(result) if isinstance(result, memoryview) else result
            descriptor = orjson.loads(result)
            self._creator = False
            self._versioned = descriptor['versioned']
        self._finish_bind_lmdb(descriptor)
        self._bind()

    def _try_bind_lmdb(self) -> Optional[Descriptor]:
        with context(self._environment, write = False, inherit = True, buffers = False):
            txn = thread.local.transaction
            result = txn.get(key = self._encoded_name, db = self._name_db)
            if result:
                obj_uuid = bytes(result) if isinstance(result, memoryview) else result
                result = txn.get(key = obj_uuid, db = self._descriptor_db)
                result = bytes(result) if isinstance(result, memoryview) else result
                descriptor = orjson.loads(result)
                if descriptor['type'] != get_qualified_class_name(self):
                    raise TypeError()
                self._uuid_bytes = obj_uuid
                self._creator = False
                self._versioned = descriptor['versioned']
                return descriptor
            return None

    def _finish_bind_lmdb(self, descriptor: Descriptor) -> None:
        for dbuid, _ in descriptor['databases']:
            self._user_db.append(get_database(dbuid))
            self._user_dbuid.append(dbuid)
        if any([db is None for db in self._user_db]):
            with context(self._environment, write = True, inherit = True, buffers = False):
                txn = thread.local.transaction
                for index, (dbuid, properties) in enumerate(descriptor['databases']):
                    if not self._user_db[index]:
                        self._user_db[index] = \
                        open_database(txn, self._environment, dbuid, properties, create = False)

    def _create_lmdb(
        self,
        properties: List[LMDBProperties],
        versioned: bool
    ) -> None:
        txn = thread.local.transaction
        obj_uuid = txn.get(key = self._encoded_name, db = self._name_db)
        if obj_uuid:
            raise ObjectExistsError()
        obj_uuid = uuid.uuid4().bytes
        assert txn.put(key = self._encoded_name, value = obj_uuid, db = self._name_db)
        assert txn.put(key = obj_uuid, value = struct.pack('@N', 0), db = self._version_db)
        basename = str(uuid.uuid4())
        descriptor = dict(
            databases = list(
                zip(
                    [
                        create_string_digest(''.join([basename, str(i)]))
                        for i in range(len(properties))
                    ],
                    properties
                )
            ),
            versioned = versioned,
            created = datetime.datetime.now(),
            type = get_qualified_class_name(self)
        )
        assert txn.put(key = obj_uuid, value = orjson.dumps(descriptor), db = self._descriptor_db)
        for dbuid, props in descriptor['databases']:
            self._user_db.append(open_database(txn, self._environment, dbuid, props, create = True))
            self._user_dbuid.append(dbuid)
        self._versioned = descriptor['versioned']
        self._uuid_bytes = obj_uuid
        self._creator = True

    def _bind(self, *args: Any) -> None:
        pass
