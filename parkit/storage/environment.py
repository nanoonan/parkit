# pylint: disable = no-member, protected-access
import atexit
import collections
import functools
import logging
import os
import struct
import threading
import uuid

from typing import (
    cast, Dict, Optional, Tuple, Union
)

import lmdb

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.cluster.manage import (
    launch_node,
    scan_nodes
)
from parkit.exceptions import (
    ContextError,
    StoragePathError,
    TransactionError
)
from parkit.profiles import get_lmdb_profiles
from parkit.typeddicts import LMDBProperties
from parkit.utility import (
    create_string_digest,
    envexists,
    getenv,
    resolve_namespace
)

logger = logging.getLogger(__name__)

environments_lock: threading.Lock = threading.Lock()

environments: Dict[
    str,
    Tuple[lmdb.Environment, lmdb._Database, lmdb._Database, lmdb._Database, lmdb._Database]
] = {}

databases_lock: threading.Lock = threading.Lock()

databases: Dict[Union[int, str], lmdb._Database] = {}

mapsizes: Dict[str, int] = \
collections.defaultdict(lambda: get_lmdb_profiles()['default']['LMDB_INITIAL_MAP_SIZE'])

def close_environments_atexit():
    with environments_lock:
        for env, _, _, _, _ in environments.values():
            try:
                env.close()
            except lmdb.Error:
                logger.exception('close environment error on pid %i', os.getpid())

atexit.register(close_environments_atexit)

def make_namespace_key(storage_path: str, namespace: str) -> str:
    return ':'.join([storage_path, namespace])

def resolve_storage_path(path: Optional[str] = None) -> str:
    if path is None:
        if thread.local.storage_path is None:
            if envexists(constants.STORAGE_PATH_ENVNAME):
                set_storage_path(getenv(constants.STORAGE_PATH_ENVNAME, str))
            else:
                raise StoragePathError()
        return thread.local.storage_path
    check_storage_path(path)
    return path

def get_storage_path() -> Optional[str]:
    try:
        return resolve_storage_path()
    except StoragePathError:
        return None

def set_storage_path(path: str):
    path = os.path.abspath(path)
    check_storage_path(path)
    thread.local.storage_path = path
    initialize_storage_path(thread.local.storage_path)
    if not envexists(constants.NODE_UID_ENVNAME) or \
    not envexists(constants.CLUSTER_UID_ENVNAME):
        if getenv(constants.POOL_AUTOSTART_ENVNAME, bool):
            cluster_uid = create_string_digest(path)
            if 'monitor' not in \
            [node_uid.split('-')[0] for node_uid in scan_nodes(cluster_uid)]:
                launch_node(
                    'monitor-{0}'.format(create_string_digest(str(uuid.uuid4()))),
                    'parkit.cluster.monitordaemon',
                    cluster_uid
                )

def check_storage_path(path: str):
    if os.path.exists(path):
        if not os.path.isdir(path):
            raise StoragePathError()
    else:
        if not os.access(os.path.dirname(path), os.W_OK):
            raise StoragePathError()
        try:
            os.makedirs(path)
        except FileExistsError:
            pass
        except OSError as exc:
            raise StoragePathError() from exc

def initialize_storage_path(storage_path: str):

    namespace_key = make_namespace_key(storage_path, constants.SETTINGS_NAMESPACE)

    if namespace_key in environments:
        return

    with environments_lock:

        if namespace_key in environments:
            return

        env_path = os.path.join(storage_path, *constants.SETTINGS_NAMESPACE.split('/'))

        check_storage_path(env_path)

        env = lmdb.open(env_path, subdir = True, create = True)

        environments[namespace_key] = (env, None, None, None, None)

        try:

            cache = {}
            txn = None
            txn = env.begin(write = True)
            cursor = txn.cursor()
            if cursor.first():
                while True:
                    cache[cursor.key().decode('utf-8')] = struct.unpack('@N', cursor.value())[0]
                    if not cursor.next():
                        break
            txn.commit()

            for namespace, size in cache.items():
                mapsizes[make_namespace_key(storage_path, namespace)] = size

        except BaseException as exc:
            if txn:
                txn.abort()
            raise TransactionError() from exc

def _set_namespace_size_threadsafe(
    size: int,
    storage_path: str,
    namespace: str
):
    with environments_lock:

        namespace_key = make_namespace_key(storage_path, namespace)

        if namespace_key in environments:

            settings_env, _, _, _, _ = environments[
                make_namespace_key(storage_path, constants.SETTINGS_NAMESPACE)
            ]

            cached_size = mapsizes[namespace_key]

            try:
                txn = None
                txn = settings_env.begin(write = True)
                assert txn.put(key = namespace.encode('utf-8'), value = struct.pack('@N', size))
                mapsizes[namespace_key] = size
                target_env, _, _, _, _ = environments[namespace_key]
                target_env.set_mapsize(size)
                txn.commit()
            except BaseException as exc:
                mapsizes[namespace_key] = cached_size
                if txn:
                    txn.abort()
                raise TransactionError() from exc

def get_namespace_size(
    storage_path: str,
    namespace: Optional[str] = None,
) -> int:
    namespace = cast(
        str,
        resolve_namespace(namespace) if namespace else constants.DEFAULT_NAMESPACE
    )
    namespace_key = make_namespace_key(storage_path, namespace)
    return mapsizes[namespace_key]

def set_namespace_size(
    size: int,
    storage_path: str,
    namespace: Optional[str] = None
):
    assert size > 0
    if thread.local.transaction:
        raise ContextError()
    namespace = resolve_namespace(namespace) if namespace else constants.DEFAULT_NAMESPACE
    _set_namespace_size_threadsafe(size, storage_path, cast(str, namespace))

def get_database_threadsafe(key: Union[int, str]) -> Optional[lmdb._Database]:
    try:
        with databases_lock:
            return databases[key]
    except KeyError:
        return None

def open_database_threadsafe(
    txn: lmdb.Transaction,
    env: lmdb.Environment,
    dbuid: str,
    properties: LMDBProperties,
    create: bool = False
) -> lmdb._Database:
    with databases_lock:
        if dbuid not in databases:
            database = env.open_db(
                txn = txn, key = dbuid.encode('utf-8'),
                integerkey = properties['integerkey'] if 'integerkey' in properties else False,
                dupsort = properties['dupsort'] if 'dupsort' in properties else False,
                dupfixed = properties['dupfixed'] if 'dupfixed' in properties else False,
                integerdup = properties['integerdup'] if 'integerdup' in properties else False,
                reverse_key = properties['reverse_key'] if 'reverse_key' in properties else False,
                create = create
            )
            databases[id(database)] = database
            databases[dbuid] = database
    return databases[dbuid]

@functools.lru_cache(None)
def get_environment_threadsafe(
    storage_path: str,
    namespace: str
) -> Tuple[
    lmdb.Environment, lmdb._Database, lmdb._Database,
    lmdb._Database, lmdb._Database
]:

    namespace_key = make_namespace_key(storage_path, namespace)

    if namespace_key not in environments:

        with environments_lock:

            if namespace_key not in environments:

                env_path = os.path.join(storage_path, *namespace.split('/'))

                check_storage_path(env_path)

                profile = get_lmdb_profiles()['default']

                env = lmdb.open(
                    env_path, subdir = True, create = True,
                    writemap = profile['LMDB_WRITE_MAP'],
                    metasync = profile['LMDB_METASYNC'],
                    map_async = profile['LMDB_MAP_ASYNC'],
                    map_size = mapsizes[namespace],
                    max_dbs = profile['LMDB_MAX_DBS'],
                    max_spare_txns = profile['LMDB_MAX_SPARE_TXNS'],
                    max_readers = profile['LMDB_MAX_READERS'],
                    readonly = profile['LMDB_READONLY'],
                    sync = profile['LMDB_SYNC'],
                    meminit = profile['LMDB_MEMINIT']
                )

                env.reader_check()

                name_db = env.open_db(
                    key = constants.NAME_DATABASE.encode('utf-8')
                )
                databases[id(name_db)] = name_db
                version_db = env.open_db(
                    key = constants.VERSION_DATABASE.encode('utf-8')
                )
                databases[id(version_db)] = version_db
                descriptor_db = env.open_db(
                    key = constants.DESCRIPTOR_DATABASE.encode('utf-8')
                )
                databases[id(descriptor_db)] = descriptor_db
                attribute_db = env.open_db(
                    key = constants.ATTRIBUTE_DATABASE.encode('utf-8')
                )
                databases[id(attribute_db)] = attribute_db

                environments[namespace_key] = \
                (env, name_db, attribute_db, version_db, descriptor_db)

    return environments[namespace_key]
