# pylint: disable = c-extension-no-member, broad-except, no-member
#
# reviewed: 6/16/21
#
import datetime
import logging
import struct
import uuid

from typing import (
    Any, Callable, Dict, List, Optional, Tuple
)

import lmdb
import orjson

import parkit.storage.threadlocal as thread

from parkit.exceptions import (
    ObjectExistsError,
    ObjectNotFoundError,
    SiteNotSpecifiedError,
    TransactionError
)
from parkit.storage.context import transaction_context
from parkit.storage.database import (
    get_database_threadsafe,
    open_database_threadsafe
)
from parkit.storage.entitymeta import EntityMeta
from parkit.storage.environment import get_environment_threadsafe

from parkit.storage.site import get_storage_path
from parkit.typeddicts import (
    Descriptor,
    LMDBProperties
)
from parkit.utility import (
    create_class,
    create_string_digest,
    get_memory_size,
    get_qualified_base_names,
    get_qualified_class_name
)

logger = logging.getLogger(__name__)

class Entity(metaclass = EntityMeta):

    __slots__ = {
        '_env', '_namedb', '_descdb', '_versdb', '_attrdb', '_userdb',
        '_uuid_bytes', '_versioned', '_site_uuid', '_create',
        '_encname', '_namespace', '_storage_path'
    }

    def __init__(
        self,
        namespace: str,
        name: str,
        /, *,
        create: bool = False,
        bind: bool = True,
        db_properties: Optional[List[LMDBProperties]] = None,
        versioned: bool = False,
        on_init: Optional[Callable[[bool], None]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        site_uuid: Optional[str] = None
    ):
        if not create and not bind:
            raise ValueError()
        if not namespace or not name:
            raise ValueError()

        super().__init__()

        self._create = False

        self._encname: bytes = name.encode('utf-8')
        self._namespace: str = namespace

        if site_uuid is not None:
            self._site_uuid = site_uuid
            self._storage_path = get_storage_path(self._site_uuid)
        else:
            if thread.local.default_site is not None:
                self._storage_path, self._site_uuid = thread.local.default_site
            else:
                raise SiteNotSpecifiedError()

        self._env: lmdb.Environment
        self._namedb: lmdb._Database
        self._attrdb: lmdb._Database
        self._versdb: lmdb._Database
        self._descdb: lmdb._Database

        _, self._env, self._namedb, self._attrdb, self._versdb, self._descdb = \
        get_environment_threadsafe(
            self._storage_path,
            self._namespace,
            create = create
        )

        self._userdb: List[lmdb._Database] = []

        self._uuid_bytes: bytes
        self._versioned: bool

        if bind:
            self.__bind_or_create(
                db_properties = db_properties if db_properties is not None else [],
                versioned = versioned,
                metadata = metadata if metadata is not None else {},
                on_init = on_init,
                create = create
            )
        else:
            self.__create_or_bind(
                db_properties = db_properties if db_properties is not None else [],
                versioned = versioned,
                metadata = metadata if metadata is not None else {},
                on_init = on_init,
                bind = bind
            )

    def __hash__(self) -> int:
        return int.from_bytes(self._uuid_bytes, 'little')

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __eq__(self, other: Any) -> bool:
        if isinstance(type(other), EntityMeta):
            return self._uuid_bytes == other._uuid_bytes
        return False

    def __getstate__(self) -> Any:
        return (
            self._site_uuid,
            self._namespace,
            self._encname.decode('utf-8'),
        )

    def __setstate__(self, from_wire: Any):
        self._site_uuid, self._namespace, name = from_wire
        self._storage_path = get_storage_path(self._site_uuid)
        self._create = False
        self._encname = name.encode('utf-8')
        _, self._env, self._namedb, self._attrdb, self._versdb, self._descdb = \
        get_environment_threadsafe(
            self._storage_path,
            self._namespace,
            create = False
        )
        self._userdb = []
        with transaction_context(self._env, write = False) as (txn, _, _):
            self._uuid_bytes = txn.get(key = self._encname, db = self._namedb)
            self._uuid_bytes = bytes(self._uuid_bytes) \
            if isinstance(self._uuid_bytes, memoryview) else self._uuid_bytes
            if self._uuid_bytes is None:
                raise ObjectNotFoundError()
            result = txn.get(key = self._uuid_bytes, db = self._descdb)
            result = bytes(result) if isinstance(result, memoryview) else result
            descriptor = orjson.loads(result)
            self._versioned = descriptor['versioned']
        self.__bind_databases(descriptor = descriptor)
        self.__class__.__initialize_class__()

    def __bind_or_create(
        self,
        *,
        db_properties: List[LMDBProperties],
        versioned: bool,
        metadata: Dict[str, Any],
        on_init: Optional[Callable[[bool], None]] = None,
        create: bool = True
    ):
        with transaction_context(self._env, write = False) as (txn, _, _):
            result = txn.get(key = self._encname, db = self._namedb)
            if result:
                obj_uuid = bytes(result) if isinstance(result, memoryview) else result
                result = txn.get(key = obj_uuid, db = self._descdb)
                result = bytes(result) if isinstance(result, memoryview) else result
                descriptor = orjson.loads(result)
                my_class_name = get_qualified_class_name(self)
                if descriptor['type'] != my_class_name:
                    try:
                        if my_class_name not in \
                        get_qualified_base_names(create_class(descriptor['type'])):
                            raise TypeError()
                    except AttributeError as exc:
                        if my_class_name != 'parkit.storage.entity.Entity':
                            raise TypeError() from exc
                self._uuid_bytes = obj_uuid
                self._versioned = descriptor['versioned']
                self.__bind_databases(descriptor = descriptor, on_init = on_init)
                return None
        if create:
            return self.__create_or_bind(
                db_properties = db_properties,
                versioned = versioned,
                metadata = metadata,
                on_init = on_init,
                bind = True
            )
        raise ObjectNotFoundError()

    def __bind_databases(
        self,
        *,
        descriptor: Descriptor,
        on_init: Optional[Callable[[bool], None]] = None
    ):
        for dbuid, _ in descriptor['databases']:
            self._userdb.append(get_database_threadsafe(dbuid))
        if any(db is None for db in self._userdb):
            with transaction_context(self._env, write = True) as (txn, _, _):
                for index, (dbuid, properties) in enumerate(descriptor['databases']):
                    if not self._userdb[index]:
                        self._userdb[index] = open_database_threadsafe(
                            txn, self._env, dbuid, properties, create = False
                        )
                if on_init:
                    on_init(False)
        else:
            if on_init:
                on_init(False)

    def __create_or_bind(
        self,
        *,
        db_properties: List[LMDBProperties],
        versioned: bool,
        metadata: Dict[str, Any],
        on_init: Optional[Callable[[bool], None]],
        bind: bool = True
    ):
        with transaction_context(self._env, write = True) as (txn, _, _):
            obj_uuid = txn.get(key = self._encname, db = self._namedb)
            if obj_uuid:
                if bind:
                    return self.__bind_or_create(
                        db_properties = db_properties,
                        versioned = versioned,
                        metadata = metadata,
                        on_init = on_init,
                        create = False
                    )
                raise ObjectExistsError()
            obj_uuid = uuid.uuid4().bytes
            assert txn.put(key = self._encname, value = obj_uuid, db = self._namedb)
            assert txn.put(key = obj_uuid, value = struct.pack('@N', 0), db = self._versdb)
            basename = str(uuid.uuid4())
            descriptor: Descriptor = dict(
                databases = list(
                    zip(
                        [
                            create_string_digest(''.join([basename, str(i)]))
                            for i in range(len(db_properties))
                        ],
                        db_properties
                    )
                ),
                uuid = str(uuid.UUID(bytes = obj_uuid)),
                versioned = versioned,
                created = str(datetime.datetime.now()),
                type = get_qualified_class_name(self),
                metadata = metadata
            )
            assert txn.put(key = obj_uuid, value = orjson.dumps(descriptor), db = self._descdb)
            for dbuid, props in descriptor['databases']:
                self._userdb.append(open_database_threadsafe(
                    txn, self._env, dbuid, props, create = True)
                )
            self._uuid_bytes = obj_uuid
            self._versioned = descriptor['versioned']
            if on_init:
                self._create = True
                on_init(True)
                self._create = False
            return None

    def _increment_version(self, cursors: thread.CursorDict):
        if not self._versioned or self._create:
            return
        cursor = cursors[self._versdb]
        if cursor.set_key(self._uuid_bytes):
            version = struct.pack('@N', struct.unpack('@N', cursor.value())[0] + 1)
            assert cursor.put(key = self._uuid_bytes, value = version)
        else:
            raise ObjectNotFoundError()

    def _abort(
        self,
        error: BaseException,
        txn: lmdb.Transaction,
        implicit = False
    ):
        try:
            try:
                if isinstance(error, lmdb.Error):
                    obj_uuid = txn.get(key = self._encname, db = self._namedb)
                    if obj_uuid != self._uuid_bytes:
                        raise ObjectNotFoundError() from error
            finally:
                if implicit:
                    txn.abort()
            if isinstance(error, lmdb.Error):
                raise TransactionError() from error
            raise error
        except lmdb.Error as exc:
            raise TransactionError() from exc

    @property
    def site_uuid(self) -> str:
        return self._site_uuid

    @property
    def storage_path(self) -> str:
        return self._storage_path

    @property
    def path(self) -> str:
        return '/'.join([
            self._namespace,
            self._encname.decode('utf-8')
        ])

    @property
    def uuid(self) -> str:
        return str(uuid.UUID(bytes = self._uuid_bytes))

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def name(self) -> str:
        return self._encname.decode('utf-8')

    @property
    def debug(self) -> Dict[str, Any]:
        self._env.reader_check()
        return dict(
            memory_size = get_memory_size(self),
            encoded_name = self._encname,
            namespace = self._namespace,
            uuid_bytes = self._uuid_bytes,
            environment = self._env,
            name_db = (id(self._namedb), self._namedb),
            attribute_db = (id(self._attrdb), self._attrdb),
            version_db = (id(self._versdb), self._versdb),
            descriptor_db = (id(self._descdb), self._descdb),
            user_db = [(id(user_db), user_db) for user_db in self._userdb],
            env_readers = self._env.readers(),
            env_stat = self._env.stat(),
            env_info = self._env.info()
        )

    @property
    def descriptor(self) -> Dict[str, Any]:
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self._env, write = False)
            cursor = cursors[self._descdb]
            if cursor.set_key(self._uuid_bytes):
                result = cursor.value()
                result = bytes(result) if isinstance(result, memoryview) else result
            else:
                raise ObjectNotFoundError()
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._abort(exc, txn, implicit)
        finally:
            if implicit and cursor:
                cursor.close()
        return orjson.loads(result)

    @property
    def metadata(self) -> Dict[str, Any]:
        return self.descriptor['metadata']

    @metadata.setter
    def metadata(self, value: Dict[str, Any]):
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self._env, write = True)
            cursor = cursors[self._descdb]
            if cursor.set_key(self._uuid_bytes):
                result = cursor.value()
                result = bytes(result) if isinstance(result, memoryview) else result
                descriptor = orjson.loads(result)
                descriptor['metadata'] = value
                assert cursor.put(
                    self._uuid_bytes, orjson.dumps(descriptor),
                    dupdata = True, overwrite = True, append = False
                )
            else:
                raise ObjectNotFoundError()
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._abort(exc, txn, implicit)

    @property
    def exists(self) -> bool:
        try:
            txn, _, _, implicit = \
            thread.local.context.get(self._env, write = False)
            obj_uuid = txn.get(key = self._encname, db = self._namedb)
            result = obj_uuid == self._uuid_bytes
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._abort(exc, txn, implicit)
        return result

    @property
    def version(self) -> int:
        if not self._versioned:
            return 0
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self._env, write = False)
            cursor = cursors[self._versdb]
            if cursor.set_key(self._uuid_bytes):
                version = cursor.value()
                version = struct.unpack('@N', version)[0]
            else:
                raise ObjectNotFoundError()
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._abort(exc, txn, implicit)
        finally:
            if implicit and cursor:
                cursor.close()
        return version

    def drop(self):
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self._env, write = True, internal = True)
            cursor = cursors[self._attrdb]
            obj_uuid = txn.get(key = self._encname, db = self._namedb)
            if obj_uuid == self._uuid_bytes:
                for database in self._userdb:
                    txn.drop(database, delete = True)
                assert txn.delete(key = self._encname, db = self._namedb)
                assert txn.delete(key = self._uuid_bytes, db = self._versdb)
                assert txn.delete(key = self._uuid_bytes, db = self._descdb)
                if cursor.set_range(self._uuid_bytes):
                    key = cursor.key()
                    key = bytes(key) if isinstance(key, memoryview) else key
                    if key.startswith(self._uuid_bytes):
                        while True:
                            if not cursor.delete():
                                break
                            key = cursor.key()
                            key = bytes(key) if isinstance(key, memoryview) else key
                            if not key.startswith(self._uuid_bytes):
                                break
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._abort(exc, txn, implicit)
