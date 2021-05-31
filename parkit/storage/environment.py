# pylint: disable = no-member, protected-access
import atexit
import collections
import functools
import logging
import os
import struct
import tempfile
import threading

from typing import (
    cast, Dict, Optional, Tuple, Union
)

import lmdb

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.exceptions import (
    ContextError,
    TransactionError
)
from parkit.profiles import get_lmdb_profiles
from parkit.typeddicts import LMDBProperties
from parkit.utility import (
    envexists,
    getenv,
    resolve_namespace,
    setenv
)

logger = logging.getLogger(__name__)

_environments_lock: threading.Lock = threading.Lock()

_environments: Dict[
    str,
    Tuple[lmdb.Environment, lmdb._Database, lmdb._Database, lmdb._Database, lmdb._Database]
] = {}

_databases_lock: threading.Lock = threading.Lock()

_databases: Dict[Union[int, str], lmdb._Database] = {}

_mapsizes: Dict[str, int] = \
collections.defaultdict(lambda: get_lmdb_profiles()['default']['LMDB_INITIAL_MAP_SIZE'])

_settings_initialized: bool = False

def close_environments_atexit():
    with _environments_lock:
        for env, _, _, _, _ in _environments.values():
            try:
                env.close()
            except lmdb.Error:
                logger.exception('Trapped error on pid %i', os.getpid())

atexit.register(close_environments_atexit)

def initialize_settings():

    if _settings_initialized:
        return

    install_path = os.path.abspath(getenv(constants.STORAGE_PATH_ENVNAME, str))
    env_path = os.path.join(install_path, *constants.SETTINGS_NAMESPACE.split('/'))
    if os.path.exists(env_path):
        if not os.path.isdir(env_path):
            raise ValueError('Namespace path exists but is not a directory')
    else:
        try:
            os.makedirs(env_path)
        except FileExistsError:
            pass
    env = lmdb.open(env_path, subdir = True, create = True)
    _environments[constants.SETTINGS_NAMESPACE] = (env, None, None, None, None)
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
        for key, value in cache.items():
            _mapsizes[key] = value
    except BaseException as exc:
        if txn:
            txn.abort()
        raise TransactionError() from exc

    _settings_initialzed = True

def _set_namespace_size_threadsafe(
    namespace: str,
    size: int
):
    assert constants.SETTINGS_NAMESPACE in _environments
    with _environments_lock:
        initialize_settings()
        env, _, _, _, _ = _environments[constants.SETTINGS_NAMESPACE]
        try:
            txn = None
            txn = env.begin(write = True)
            assert txn.put(key = namespace.encode('utf-8'), value = struct.pack('@N', size))
            txn.commit()
            _mapsizes[namespace] = size
            if namespace in _environments:
                env, _, _, _, _ = _environments[namespace]
                env.set_mapsize(size)
        except BaseException as exc:
            if txn:
                txn.abort()
            raise TransactionError() from exc

def get_namespace_size(
    namespace: Optional[str] = None
) -> int:
    namespace = cast(
        str,
        resolve_namespace(namespace) if namespace else constants.DEFAULT_NAMESPACE
    )
    return _mapsizes[namespace]

def set_namespace_size(
    size: int,
    /, *,
    namespace: Optional[str] = None
):
    if thread.local.transaction:
        raise ContextError('Cannot set namespace size in a transaction')
    if size <= 0:
        raise ValueError('Size must be positive')
    namespace = resolve_namespace(namespace) if namespace else constants.DEFAULT_NAMESPACE
    _set_namespace_size_threadsafe(cast(str, namespace), size)

def get_database_threadsafe(key: Union[int, str]) -> Optional[lmdb._Database]:
    try:
        with _databases_lock:
            return _databases[key]
    except KeyError:
        return None

def open_database_threadsafe(
    txn: lmdb.Transaction, env: lmdb.Environment, dbuid: str,
    properties: LMDBProperties, create: bool = False
) -> lmdb._Database:
    with _databases_lock:
        if dbuid not in _databases:
            database = env.open_db(
                txn = txn, key = dbuid.encode('utf-8'),
                integerkey = properties['integerkey'] if 'integerkey' in properties else False,
                dupsort = properties['dupsort'] if 'dupsort' in properties else False,
                dupfixed = properties['dupfixed'] if 'dupfixed' in properties else False,
                integerdup = properties['integerdup'] if 'integerdup' in properties else False,
                reverse_key = properties['reverse_key'] if 'reverse_key' in properties else False,
                create = create
            )
            _databases[id(database)] = database
            _databases[dbuid] = database
    return _databases[dbuid]

@functools.lru_cache(None)
def get_environment_threadsafe(namespace: str) -> \
Tuple[lmdb.Environment, lmdb._Database, lmdb._Database, lmdb._Database, lmdb._Database]:
    if namespace not in _environments:
        with _environments_lock:
            if namespace not in _environments:
                if not envexists(constants.STORAGE_PATH_ENVNAME):
                    try:
                        os.makedirs(
                            os.path.join(
                                tempfile.gettempdir(),
                                constants.PARKIT_TEMP_INSTALLATION_DIRNAME
                            )
                        )
                    except FileExistsError:
                        pass
                    storage_path = os.path.abspath(os.path.join(
                        tempfile.gettempdir(), constants.PARKIT_TEMP_INSTALLATION_DIRNAME
                    ))
                    setenv(constants.STORAGE_PATH_ENVNAME, storage_path)
                else:
                    storage_path = getenv(constants.STORAGE_PATH_ENVNAME, str)
                env_path = os.path.join(storage_path, *namespace.split('/'))
                if os.path.exists(env_path):
                    if not os.path.isdir(env_path):
                        raise ValueError('Namespace path exists but is not a directory')
                else:
                    try:
                        os.makedirs(env_path)
                    except FileExistsError:
                        pass
                initialize_settings()
                profile = get_lmdb_profiles()['default']
                env = lmdb.open(
                    env_path, subdir = True, create = True,
                    writemap = profile['LMDB_WRITE_MAP'],
                    metasync = profile['LMDB_METASYNC'],
                    map_async = profile['LMDB_MAP_ASYNC'],
                    map_size = _mapsizes[namespace],
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
                _databases[id(name_db)] = name_db
                version_db = env.open_db(
                    key = constants.VERSION_DATABASE.encode('utf-8')
                )
                _databases[id(version_db)] = version_db
                descriptor_db = env.open_db(
                    key = constants.DESCRIPTOR_DATABASE.encode('utf-8')
                )
                _databases[id(descriptor_db)] = descriptor_db
                attribute_db = env.open_db(
                    key = constants.ATTRIBUTE_DATABASE.encode('utf-8')
                )
                _databases[id(attribute_db)] = attribute_db
                _environments[namespace] = (env, name_db, attribute_db, version_db, descriptor_db)
    return _environments[namespace]
