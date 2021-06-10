# pylint: disable = c-extension-no-member, broad-except, no-member, too-many-instance-attributes, protected-access, too-few-public-methods
import datetime
import logging
import struct
import typing
import uuid

from typing import (
    Any, Callable, Dict, List, Optional, Protocol, Tuple
)

import lmdb
import orjson

import parkit.constants as constants
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

from parkit.storage.site import (
    get_site_name,
    get_site_uuid
)
from parkit.storage.threadlocal import StoragePath
from parkit.typeddicts import (
    Descriptor,
    LMDBProperties
)
from parkit.utility import (
    create_string_digest,
    getenv,
    get_memory_size,
    get_qualified_class_name
)

logger = logging.getLogger(__name__)

class Entity(metaclass = EntityMeta):

    __slots__ = {
        '__env', '__encname', '__namespace', '__namedb',
        '__descdb', '__versdb', '__attrdb', '__userdb',
        '__uuidbytes', '__vers', '__siteuuid', '__anon',
        '__creat'
    }

    def __init__(
        self,
        namespace: str,
        name: str,
        /, *,
        anonymous: bool = False,
        db_properties: Optional[List[LMDBProperties]] = None,
        create: bool = True,
        bind: bool = True,
        versioned: bool = False,
        type_check: bool = True,
        on_init: Optional[Callable[[bool], None]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        site: Optional[str] = None
    ):
        super().__init__()

        self.__creat = False

        if not namespace or not name:
            raise ValueError()

        if not create and not bind:
            raise ValueError()

        self.__encname: bytes = name.encode('utf-8')
        self.__namespace: str = namespace

        if site:
            self.__siteuuid = get_site_uuid(site)
        elif thread.local.storage_path:
            self.__siteuuid = thread.local.storage_path.site_uuid
        else:
            raise SiteNotSpecifiedError()

        self.__env: lmdb.Environment
        self.__namedb: lmdb._Database
        self.__attrdb: lmdb._Database
        self.__versdb: lmdb._Database
        self.__descdb: lmdb._Database

        _, self.__env, self.__namedb, self.__attrdb, self.__versdb, self.__descdb = \
        get_environment_threadsafe(
            StoragePath(site_uuid = self.__siteuuid).path,
            self.__namespace
        )

        self.__userdb: List[lmdb._Database] = []

        self.__uuidbytes: bytes
        self.__vers: bool
        self.__anon: bool

        descriptor = None
        if bind:
            descriptor = self.__try_bind_lmdb(type_check)

        if descriptor:
            self.__finish_bind_lmdb(descriptor)
            if on_init:
                on_init(False)
        elif create:
            try:
                self.__creat = True
                with transaction_context(self.__env, write = True):
                    self.__create_lmdb(
                        db_properties if db_properties else [],
                        anonymous, versioned,
                        metadata if metadata else {}
                    )
                    if on_init:
                        on_init(True)
            finally:
                self.__creat = False
        else:
            raise ObjectNotFoundError()

    def __hash__(self) -> int:
        return int.from_bytes(self.__uuidbytes, 'little')

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __eq__(self, other: Any) -> bool:
        if isinstance(type(other), EntityMeta):
            return self.__uuidbytes == other.__uuidbytes
        return False

    def __getstate__(self) -> Tuple[str, str, str]:
        return (
            self.__siteuuid,
            self.__namespace,
            self.__encname.decode('utf-8'),
        )

    def __setstate__(self, from_wire: Tuple[str, str, str]):
        self.__siteuuid, self.__namespace, name = from_wire
        self.__creat = False
        self.__encname = name.encode('utf-8')
        _, self.__env, self.__namedb, self.__attrdb, self.__versdb, self.__descdb = \
        get_environment_threadsafe(
            StoragePath(site_uuid = self.__siteuuid).path,
            self.__namespace
        )
        self.__userdb = []
        with transaction_context(self.__env, write = False) as (txn, _, _):
            self.__uuidbytes = txn.get(key = self.__encname, db = self.__namedb)
            self.__uuidbytes = bytes(self.__uuidbytes) \
            if isinstance(self.__uuidbytes, memoryview) else self.__uuidbytes
            if self.__uuidbytes is None:
                raise ObjectNotFoundError()
            result = txn.get(key = self.__uuidbytes, db = self.__descdb)
            result = bytes(result) if isinstance(result, memoryview) else result
            descriptor = orjson.loads(result)
            self.__vers = descriptor['versioned']
            self.__anon = descriptor['anonymous']
        self.__finish_bind_lmdb(descriptor)
        self.__class__.__initialize_class__()

    def __try_bind_lmdb(
        self,
        type_check: bool
    ) -> Optional[Descriptor]:
        with transaction_context(self.__env, write = False) as (txn, _, _):
            result = txn.get(key = self.__encname, db = self.__namedb)
            if result:
                obj_uuid = bytes(result) if isinstance(result, memoryview) else result
                result = txn.get(key = obj_uuid, db = self.__descdb)
                result = bytes(result) if isinstance(result, memoryview) else result
                descriptor = orjson.loads(result)
                if type_check:
                    if descriptor['type'] != get_qualified_class_name(self):
                        raise TypeError()
                self.__uuidbytes = obj_uuid
                self.__vers = descriptor['versioned']
                self.__anon = descriptor['anonymous']
                return descriptor
            return None

    def __finish_bind_lmdb(self, descriptor: Descriptor):
        for dbuid, _ in descriptor['databases']:
            self.__userdb.append(get_database_threadsafe(dbuid))
        if any(db is None for db in self.__userdb):
            with transaction_context(self.__env, write = True) as (txn, _, _):
                for index, (dbuid, properties) in enumerate(descriptor['databases']):
                    if not self.__userdb[index]:
                        self.__userdb[index] = \
                        open_database_threadsafe(
                            txn, self.__env, dbuid, properties, create = False
                        )

    def __create_lmdb(
        self,
        db_properties: List[LMDBProperties],
        anonymous: bool,
        versioned: bool,
        metadata: Dict[str, Any]
    ):
        txn, _, _, _ = thread.local.context.get(self.__env, write = True)
        obj_uuid = txn.get(key = self.__encname, db = self.__namedb)
        if obj_uuid:
            raise ObjectExistsError()
        obj_uuid = uuid.uuid4().bytes
        assert txn.put(key = self.__encname, value = obj_uuid, db = self.__namedb)
        assert txn.put(key = obj_uuid, value = struct.pack('@N', 0), db = self.__versdb)
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
            origin = getenv(constants.PROCESS_UUID_ENVNAME, str),
            uuid = str(uuid.UUID(bytes = obj_uuid)),
            anonymous = anonymous,
            versioned = versioned,
            created = str(datetime.datetime.now()),
            type = get_qualified_class_name(self),
            metadata = metadata
        )
        assert txn.put(key = obj_uuid, value = orjson.dumps(descriptor), db = self.__descdb)
        for dbuid, props in descriptor['databases']:
            self.__userdb.append(open_database_threadsafe(
                txn, self.__env, dbuid, props, create = True)
            )
        self.__uuidbytes = obj_uuid
        self.__vers = descriptor['versioned']
        self.__anon = descriptor['anonymous']

    def __increment_version(self, cursors: thread.CursorDict):
        if not self.__vers or self.__creat:
            return
        cursor = cursors[self.__versdb]
        if cursor.set_key(self.__uuidbytes):
            version = struct.pack('@N', struct.unpack('@N', cursor.value())[0] + 1)
            assert cursor.put(key = self.__uuidbytes, value = version)
        else:
            raise ObjectNotFoundError()

    def __abort(
        self,
        error: BaseException,
        txn: lmdb.Transaction,
        implicit = False
    ):
        try:
            if isinstance(error, lmdb.Error):
                obj_uuid = txn.get(key = self.__encname, db = self.__namedb)
                if obj_uuid != self.__uuidbytes:
                    raise ObjectNotFoundError() from error
        except lmdb.Error:
            pass
        finally:
            if implicit:
                try:
                    txn.abort()
                except lmdb.Error:
                    pass
        if isinstance(error, lmdb.Error):
            raise TransactionError() from error
        raise error

    @property
    def site(self) -> str:
        return get_site_name(self.__siteuuid)

    @property
    def site_uuid(self) -> str:
        return self.__siteuuid

    @property
    def storage_path(self) -> str:
        return StoragePath(site_uuid = self.__siteuuid).path

    @property
    def path(self) -> str:
        separator = '.' if '.' in self.__namespace else '/'
        return separator.join([
            self.__namespace,
            self.__encname.decode('utf-8')
        ])

    @property
    def uuid(self) -> str:
        return str(uuid.UUID(bytes = self.__uuidbytes))

    @property
    def versioned(self) -> bool:
        return self.__vers

    @property
    def anonymous(self) -> bool:
        return self.__anon

    @property
    def created(self) -> str:
        return self.descriptor['created']

    @property
    def namespace(self) -> str:
        return self.__namespace

    @property
    def name(self) -> str:
        return self.__encname.decode('utf-8')

    @property
    def debug(self) -> Dict[str, Any]:
        self.__env.reader_check()
        return dict(
            memory_size = get_memory_size(self),
            encoded_name = self.__encname,
            namespace = self.__namespace,
            uuid_bytes = self.__uuidbytes,
            environment = self.__env,
            name_db = (id(self.__namedb), self.__namedb),
            attribute_db = (id(self.__attrdb), self.__attrdb),
            version_db = (id(self.__versdb), self.__versdb),
            descriptor_db = (id(self.__descdb), self.__descdb),
            user_db = [(id(user_db), user_db) for user_db in self.__userdb],
            env_readers = self.__env.readers(),
            env_stat = self.__env.stat(),
            env_info = self.__env.info()
        )

    @property
    def descriptor(self) -> Dict[str, Any]:
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self.__env, write = False)

            cursor = cursors[self.__descdb]

            if cursor.set_key(self.__uuidbytes):
                result = cursor.value()
                result = bytes(result) if isinstance(result, memoryview) else result
            else:
                raise ObjectNotFoundError()
            if implicit:
                txn.commit()
        except BaseException as exc:
            self.__abort(exc, txn, implicit)
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
            thread.local.context.get(self.__env, write = True)

            cursor = cursors[self.__descdb]

            if cursor.set_key(self.__uuidbytes):
                result = cursor.value()
                result = bytes(result) if isinstance(result, memoryview) else result
                descriptor = orjson.loads(result)
                descriptor['metadata'] = value
                cursor.put(
                    self.__uuidbytes, orjson.dumps(descriptor),
                    dupdata = True, overwrite = True, append = False
                )
            else:
                raise ObjectNotFoundError()
            if implicit:
                txn.commit()
        except BaseException as exc:
            self.__abort(exc, txn, implicit)
        finally:
            if implicit and cursor:
                cursor.close()

    @property
    def exists(self) -> bool:
        try:
            txn, _, _, implicit = \
            thread.local.context.get(self.__env, write = False)
            obj_uuid = txn.get(key = self.__encname, db = self.__namedb)
            result = obj_uuid == self.__uuidbytes
            if implicit:
                txn.commit()
        except BaseException as exc:
            self.__abort(exc, txn, implicit)
        return result

    @property
    def version(self) -> Optional[int]:
        if not self.__vers:
            return None
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self.__env, write = False)
            cursor = cursors[self.__versdb]
            if cursor.set_key(self.__uuidbytes):
                version = cursor.value()
                version = struct.unpack('@N', version)[0]
            else:
                raise ObjectNotFoundError()
            if implicit:
                txn.commit()
        except BaseException as exc:
            self.__abort(exc, txn, implicit)
        finally:
            if implicit and cursor:
                cursor.close()
        return version

    def drop(self):
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self.__env, write = True)
            cursor = cursors[self.__attrdb]
            obj_uuid = txn.get(key = self.__encname, db = self.__namedb)
            if obj_uuid == self.__uuidbytes:
                for database in self.__userdb:
                    txn.drop(database, delete = True)
                assert txn.delete(key = self.__encname, db = self.__namedb)
                assert txn.delete(key = self.__uuidbytes, db = self.__versdb)
                assert txn.delete(key = self.__uuidbytes, db = self.__descdb)
                if cursor.set_range(self.__uuidbytes):
                    key = cursor.key()
                    key = bytes(key) if isinstance(key, memoryview) else key
                    if key.startswith(self.__uuidbytes):
                        while True:
                            if not cursor.delete():
                                break
                            key = cursor.key()
                            key = bytes(key) if isinstance(key, memoryview) else key
                            if not key.startswith(self.__uuidbytes):
                                break
            if implicit:
                txn.commit()
        except BaseException as exc:
            self.__abort(exc, txn, implicit)
        finally:
            if implicit and cursor:
                cursor.close()

@typing.runtime_checkable
class EntityWrapper(Protocol):

    @property
    def entity(self) -> Entity:
        """Return wrapped Entity instance"""
