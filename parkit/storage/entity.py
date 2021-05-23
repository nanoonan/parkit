# pylint: disable = c-extension-no-member, broad-except, no-member, too-many-instance-attributes, protected-access
import datetime
import logging
import struct
import traceback
import uuid

from typing import (
    Any, Callable, Dict, List, Optional, Tuple
)

import lmdb
import orjson

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.exceptions import (
    ObjectExistsError,
    ObjectNotFoundError,
    TransactionError
)
from parkit.storage.context import context
from parkit.storage.entitymeta import EntityMeta
from parkit.storage.environment import (
    get_database_threadsafe,
    open_database_threadsafe,
    get_environment_threadsafe
)
from parkit.typeddicts import (
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

    __slots__ = {
        '__env', '__encname', '__namespace', '__namedb',
        '__descdb', '__versdb', '__attrdb', '__userdb',
        '__vers', '__creator', '__uuidbytes'
    }

    def __init__(
        self,
        name: str,
        /, *,
        properties: Optional[List[LMDBProperties]] = None,
        namespace: Optional[str] = None,
        create: bool = True,
        bind: bool = True,
        versioned: bool = False,
        typecheck: bool = True,
        on_create: Optional[Callable[[], None]] = None,
        custom_descriptor: Optional[Dict[str, Any]] = None
    ):

        if not create and not bind:
            raise ValueError()

        self.__encname: bytes = name.encode('utf-8')
        self.__namespace: str = namespace if namespace else constants.DEFAULT_NAMESPACE

        self.__env: lmdb.Environment
        self.__namedb: lmdb._Database
        self.__attrdb: lmdb._Database
        self.__versdb: lmdb._Database
        self.__descdb: lmdb._Database

        self.__env, self.__namedb, self.__attrdb, self.__versdb, \
        self.__descdb = get_environment_threadsafe(self.__namespace)
        self.__userdb: List[lmdb._Database] = []

        self.__vers: bool
        self.__creator: bool
        self.__uuidbytes: bytes

        if bind:
            descriptor = self.__try_bind_lmdb(typecheck)

        if descriptor:
            self.__finish_bind_lmdb(descriptor)
        elif create:
            with context(self.__env, write = True, inherit = True, buffers = False):
                self.__create_lmdb(properties if properties else [], versioned, custom_descriptor)
                if on_create:
                    on_create()
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

    def __getstate__(self) -> Tuple[str, str, str, str]:
        return (
            get_qualified_class_name(self),
            self.__namespace,
            self.__encname.decode('utf-8'),
            str(uuid.UUID(bytes = self.__uuidbytes))
        )

    def __setstate__(self, from_wire: Tuple[str, str, str, str]):
        _, self.__namespace, name, uuidstr = from_wire
        self.__encname = name.encode('utf-8')
        self.__uuidbytes = uuid.UUID(uuidstr).bytes
        self.__env, self.__namedb, self.__attrdb, self.__versdb, \
        self.__descdb = get_environment_threadsafe(self.__namespace)
        self.__userdb = []
        with context(self.__env, write = False, inherit = True, buffers = False):
            txn = thread.local.transaction
            obj_uuid = txn.get(key = self.__encname, db = self.__namedb)
            if obj_uuid != self.__uuidbytes:
                raise ObjectNotFoundError()
            result = txn.get(key = obj_uuid, db = self.__descdb)
            result = bytes(result) if isinstance(result, memoryview) else result
            descriptor = orjson.loads(result)
            self.__creator = False
            self.__vers = descriptor['versioned']
        self.__finish_bind_lmdb(descriptor)
        self.__class__.__initialize_class__()

    def __try_bind_lmdb(
        self,
        typecheck: bool
    ) -> Optional[Descriptor]:
        with context(self.__env, write = False, inherit = True, buffers = False):
            txn = thread.local.transaction
            result = txn.get(key = self.__encname, db = self.__namedb)
            if result:
                obj_uuid = bytes(result) if isinstance(result, memoryview) else result
                result = txn.get(key = obj_uuid, db = self.__descdb)
                result = bytes(result) if isinstance(result, memoryview) else result
                descriptor = orjson.loads(result)
                if typecheck:
                    if descriptor['type'] != get_qualified_class_name(self):
                        raise TypeError()
                self.__uuidbytes = obj_uuid
                self.__creator = False
                self.__vers = descriptor['versioned']
                return descriptor
            return None

    def __finish_bind_lmdb(self, descriptor: Descriptor):
        for dbuid, _ in descriptor['databases']:
            self.__userdb.append(get_database_threadsafe(dbuid))
        if any(db is None for db in self.__userdb):
            with context(self.__env, write = True, inherit = True, buffers = False):
                txn = thread.local.transaction
                for index, (dbuid, properties) in enumerate(descriptor['databases']):
                    if not self.__userdb[index]:
                        self.__userdb[index] = \
                        open_database_threadsafe(
                            txn, self.__env, dbuid, properties, create = False
                        )

    def __create_lmdb(
        self,
        properties: List[LMDBProperties],
        versioned: bool,
        custom_descriptor: Optional[Dict[str, Any]]
    ):
        txn = thread.local.transaction
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
        assert txn.put(key = obj_uuid, value = orjson.dumps(descriptor), db = self.__descdb)
        for dbuid, props in descriptor['databases']:
            self.__userdb.append(open_database_threadsafe(
                txn, self.__env, dbuid, props, create = True)
            )
        self.__vers = descriptor['versioned']
        self.__uuidbytes = obj_uuid
        self.__creator = True

    def __abort(
        self,
        exc_value: BaseException,
        txn: Optional[lmdb.Transaction] = None,
        check_exists = True
    ):
        abort = True
        if not txn:
            abort = False
            txn = thread.local.transaction
        try:
            if check_exists and isinstance(exc_value, lmdb.Error):
                obj_uuid = txn.get(key = self.__encname, db = self.__namedb)
                if obj_uuid != self.__uuidbytes:
                    raise ObjectNotFoundError() from exc_value
        except lmdb.Error as exc:
            raise TransactionError() from exc
        finally:
            if abort:
                try:
                    txn.abort()
                except lmdb.Error as exc:
                    raise TransactionError() from exc
        if isinstance(exc_value, lmdb.Error):
            raise TransactionError() from exc_value
        raise exc_value

    @property
    def path(self) -> str:
        return '/'.join([
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
    def creator(self) -> bool:
        return self.__creator

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
            versioned = self.__vers,
            creator = self.__creator,
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
    def descriptor(self) -> Descriptor:
        try:
            txn = None
            cursor = thread.local.cursors[id(self.__descdb)]
            if not cursor:
                txn = self.__env.begin()
                cursor = txn.cursor(db = self.__descdb)
            if cursor.set_key(self.__uuidbytes):
                result = cursor.value()
                result = bytes(result) if isinstance(result, memoryview) else result
            else:
                raise ObjectNotFoundError()
            if txn:
                txn.commit()
        except BaseException as exc:
            self.__abort(exc, txn, False)
        finally:
            if txn and cursor:
                cursor.close()
        return orjson.loads(result)

    def increment_version(self, use_transaction: Optional[lmdb.Transaction] = None):
        if not self.__vers:
            return
        try:
            txn = cursor = None
            if use_transaction:
                txn = use_transaction
                cursor = txn.cursor(db = self.__versdb)
            else:
                cursor = thread.local.cursors[id(self.__versdb)]
                if not cursor:
                    txn = self.__env.begin(write = True)
                    cursor = txn.cursor(db = self.__versdb)
            if cursor.set_key(self.__uuidbytes):
                version = struct.pack('@N', struct.unpack('@N', cursor.value())[0] + 1)
                assert cursor.put(key = self.__uuidbytes, value = version)
            else:
                raise ObjectNotFoundError()
            if txn and not use_transaction:
                txn.commit()
        except BaseException as exc:
            traceback.print_exc()
            self.__abort(exc, txn, False)
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
                txn = self.__env.begin()
            obj_uuid = txn.get(key = self.__encname, db = self.__namedb)
            result = obj_uuid == self.__uuidbytes
            if implicit:
                txn.commit()
        except BaseException as exc:
            self.__abort(exc, txn if implicit else None, False)
        return result

    @property
    def version(self) -> int:
        try:
            txn = None
            cursor = thread.local.cursors[id(self.__versdb)]
            if not cursor:
                txn = self.__env.begin()
                cursor = txn.cursor(db = self.__versdb)
            if cursor.set_key(self.__uuidbytes):
                version = cursor.value()
                version = struct.unpack('@N', version)[0]
            else:
                raise ObjectNotFoundError()
            if txn:
                txn.commit()
        except BaseException as exc:
            self.__abort(exc, txn, False)
        finally:
            if txn and cursor:
                cursor.close()
        return version

    def drop(self):
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self.__attrdb)]
            implicit = False
            if not cursor:
                implicit = True
                txn = self.__env.begin(write = True)
                cursor = txn.cursor(db = self.__attrdb)
            else:
                txn = thread.local.transaction
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
            self.__abort(exc, txn if implicit else None, False)
        finally:
            if implicit and cursor:
                cursor.close()
