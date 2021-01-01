# pylint: disable = c-extension-no-member, broad-except, no-member, too-many-instance-attributes
import datetime
import logging
import struct
import uuid

from typing import (
    Any, Callable, Dict, List, Optional, Tuple
)

import lmdb
import orjson

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.exceptions import (
    abort,
    ObjectExistsError,
    ObjectNotFoundError
)
from parkit.storage.context import context
from parkit.storage.entitymeta import EntityMeta
from parkit.storage.environment import (
    get_database_threadsafe,
    open_database_threadsafe,
    get_environment_threadsafe
)
from parkit.types import (
    Descriptor,
    LMDBProperties
)
from parkit.utility import (
    create_string_digest,
    get_memory_size,
    get_qualified_class_name
)

logger = logging.getLogger(__name__)

class Entity(metaclass = EntityMeta):

    __slots__ = (
        '_environment', '_encoded_name', '_namespace', '_name_db',
        '_descriptor_db', '_version_db', '_attribute_db',
        '_user_db', '_versioned', '_creator', '_uuid_bytes'
    )

    def __init__(
        self,
        name: str,
        properties: List[LMDBProperties],
        namespace: Optional[str] = None,
        create: bool = True,
        bind: bool = True,
        versioned: bool = False,
        on_create: Optional[Callable[[], None]] = None,
        custom_descriptor: Optional[Dict[str, Any]] = None
    ) -> None:

        if not create and not bind:
            raise ValueError()

        self._encoded_name: bytes = name.encode('utf-8')
        self._namespace: str = namespace if namespace else constants.DEFAULT_NAMESPACE

        self._environment: lmdb.Environment
        self._name_db: lmdb._Database
        self._attribute_db: lmdb._Database
        self._version_db: lmdb._Database
        self._descriptor_db: lmdb._Database

        self._environment, self._name_db, self._attribute_db, self._version_db, \
        self._descriptor_db = get_environment_threadsafe(self._namespace)
        self._user_db: List[lmdb._Database] = []

        self._versioned: bool
        self._creator: bool
        self._uuid_bytes: bytes

        if bind:
            descriptor = self._try_bind_lmdb()

        if descriptor:
            self._finish_bind_lmdb(descriptor)
        elif create:
            with context(self._environment, write = True, inherit = True, buffers = False):
                self._create_lmdb(properties, versioned, custom_descriptor)
                print('on_create', on_create, type(on_create))
                if on_create:
                    on_create()
        else:
            raise ObjectNotFoundError()

    def __hash__(self) -> int:
        return int.from_bytes(self._uuid_bytes, 'little')

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __eq__(self, other: Any) -> bool:
        if isinstance(type(other), EntityMeta):
            return self._uuid_bytes == other._uuid_bytes
        return False

    def __getstate__(self) -> Tuple[str, str, str, str]:
        return (
            get_qualified_class_name(self),
            self._namespace,
            self._encoded_name.decode('utf-8'),
            str(uuid.UUID(bytes = self._uuid_bytes))
        )

    def __setstate__(self, from_wire: Tuple[str, str, str, str]) -> None:
        _, self._namespace, name, uuidstr = from_wire
        self._encoded_name = name.encode('utf-8')
        self._uuid_bytes = uuid.UUID(uuidstr).bytes
        self._environment, self._name_db, self._attribute_db, self._version_db, \
        self._descriptor_db = get_environment_threadsafe(self._namespace)
        self._user_db = []

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

        self.__class__.__initialize_class__()

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
            self._user_db.append(get_database_threadsafe(dbuid))
        if any([db is None for db in self._user_db]):
            with context(self._environment, write = True, inherit = True, buffers = False):
                txn = thread.local.transaction
                for index, (dbuid, properties) in enumerate(descriptor['databases']):
                    if not self._user_db[index]:
                        self._user_db[index] = \
                        open_database_threadsafe(
                            txn, self._environment, dbuid, properties, create = False
                        )

    def _create_lmdb(
        self,
        properties: List[LMDBProperties],
        versioned: bool,
        custom_descriptor: Optional[Dict[str, Any]]
    ) -> None:
        txn = thread.local.transaction
        obj_uuid = txn.get(key = self._encoded_name, db = self._name_db)
        if obj_uuid:
            raise ObjectExistsError()
        obj_uuid = uuid.uuid4().bytes
        assert txn.put(key = self._encoded_name, value = obj_uuid, db = self._name_db)
        assert txn.put(key = obj_uuid, value = struct.pack('@N', 0), db = self._version_db)
        basename = str(uuid.uuid4())
        descriptor: Descriptor = dict(
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
            type = get_qualified_class_name(self),
            custom = custom_descriptor if custom_descriptor else {}
        )
        assert txn.put(key = obj_uuid, value = orjson.dumps(descriptor), db = self._descriptor_db)
        for dbuid, props in descriptor['databases']:
            self._user_db.append(open_database_threadsafe(
                txn, self._environment, dbuid, props, create = True)
            )
        self._versioned = descriptor['versioned']
        self._uuid_bytes = obj_uuid
        self._creator = True

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

    @property
    def debug(self) -> Dict[str, Any]:
        return dict(
            memory_size = get_memory_size(self),
            encoded_name = self._encoded_name,
            namespace = self._namespace,
            uuid_bytes = self._uuid_bytes,
            versioned = self._versioned,
            creator = self._creator,
            environment = self._environment,
            name_db = (id(self._name_db), self._name_db),
            attribute_db = (id(self._attribute_db), self._attribute_db),
            version_db = (id(self._version_db), self._version_db),
            descriptor_db = (id(self._descriptor_db), self._descriptor_db),
            user_db = [(id(user_db), user_db) for user_db in self._user_db]
        )

    @property
    def descriptor(self) -> Descriptor:
        try:
            txn = None
            cursor = thread.local.cursors[id(self._descriptor_db)]
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

    def increment_version(self, use_transaction: Optional[lmdb.Transaction] = None) -> None:
        if not self._versioned:
            return
        try:
            txn = None
            if use_transaction:
                txn = use_transaction
                cursor = txn.cursor(db = self._version_db)
            else:
                cursor = thread.local.cursors[id(self._version_db)]
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
            if implicit and txn:
                txn.abort()
            abort(exc)
        return result

    @property
    def version(self) -> int:
        try:
            txn = None
            cursor = thread.local.cursors[id(self._version_db)]
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
            cursor = thread.local.cursors[id(self._attribute_db)]
            implicit = False
            if not cursor:
                implicit = True
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._attribute_db)
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
            if implicit and txn:
                txn.abort()
            abort(exc)
        finally:
            if implicit and cursor:
                cursor.close()
