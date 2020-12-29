# pylint: disable = no-member, protected-access
import atexit
import functools
import logging
import os
import threading

from typing import (
    Dict, Optional, Tuple, Union
)

import lmdb

import parkit.constants as constants

from parkit.exceptions import log
from parkit.profiles import get_lmdb_profiles
from parkit.storage.types import LMDBProperties
from parkit.utility import getenv

logger = logging.getLogger(__name__)

_environments_lock: threading.Lock = threading.Lock()

_environments: Dict[
    str,
    Tuple[lmdb.Environment, lmdb._Database, lmdb._Database, lmdb._Database, lmdb._Database]
] = {}

_databases_lock: threading.Lock = threading.Lock()

_databases: Dict[Union[int, str], lmdb._Database] = {}

def close_environments_atexit() -> None:
    with _environments_lock:
        for env, _, _, _, _ in _environments.values():
            try:
                env.close()
            except lmdb.Error as exc:
                log(exc)

atexit.register(close_environments_atexit)

def get_database_threadsafe(key: Union[int, str]) -> Optional[lmdb._Database]:
    try:
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
                install_path = os.path.abspath(getenv(constants.INSTALL_PATH_ENVNAME))
                env_path = os.path.join(install_path, *namespace.split('/'))
                if os.path.exists(env_path):
                    if not os.path.isdir(env_path):
                        raise ValueError('Namespace path exists but is not a directory')
                else:
                    try:
                        os.makedirs(env_path)
                    except FileExistsError:
                        pass
                if namespace.startswith(constants.PERSISTENT_NAMESPACE):
                    profile = get_lmdb_profiles()['persistent']
                else:
                    profile = get_lmdb_profiles()['volatile']
                env = lmdb.open(
                    env_path, subdir = True, create = True,
                    writemap = profile[constants.LMDB_WRITE_MAP_ENVNAME],
                    metasync = profile[constants.LMDB_METASYNC_ENVNAME],
                    map_async = profile[constants.LMDB_MAP_ASYNC_ENVNAME],
                    map_size = profile[constants.LMDB_MAP_SIZE_ENVNAME],
                    max_dbs = profile[constants.LMDB_MAX_DBS_ENVNAME],
                    max_spare_txns = profile[constants.LMDB_MAX_SPARE_TXNS_ENVNAME],
                    max_readers = profile[constants.LMDB_MAX_READERS_ENVNAME],
                    readonly = profile[constants.LMDB_READONLY_ENVNAME],
                    sync = profile[constants.LMDB_SYNC_ENVNAME],
                    meminit = profile[constants.LMDB_MEMINIT_ENVNAME]
                )
                name_db = env.open_db(
                    key = constants.NAME_DATABASE.encode('utf-8'), integerkey = False
                )
                _databases[id(name_db)] = name_db
                version_db = env.open_db(
                    key = constants.VERSION_DATABASE.encode('utf-8'), integerkey = False
                )
                _databases[id(version_db)] = version_db
                descriptor_db = env.open_db(
                    key = constants.DESCRIPTOR_DATABASE.encode('utf-8'), integerkey = False
                )
                _databases[id(descriptor_db)] = descriptor_db
                attribute_db = env.open_db(
                    key = constants.ATTRIBUTE_DATABASE.encode('utf-8'), integerkey = False
                )
                _databases[id(attribute_db)] = attribute_db
                _environments[namespace] = (env, name_db, attribute_db, version_db, descriptor_db)
    return _environments[namespace]
