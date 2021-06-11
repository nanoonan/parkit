# pylint: disable = no-member, protected-access
import atexit
import functools
import logging
import os
import threading
import uuid

from typing import (
    Dict, Tuple
)

import filelock
import lmdb

import parkit.constants as constants

from parkit.exceptions import (
    StoragePathError,
    TransactionError
)
from parkit.profiles import get_lmdb_profiles
from parkit.storage.database import databases
from parkit.utility import getenv

logger = logging.getLogger(__name__)

environments_lock: threading.Lock = threading.Lock()

environments: Dict[
    str,
    Tuple[
        str, lmdb.Environment, lmdb._Database, lmdb._Database,
        lmdb._Database, lmdb._Database
    ]
] = {}

def close_environments_atexit():
    with environments_lock:
        for _, env, _, _, _, _ in environments.values():
            try:
                env.close()
            except lmdb.Error:
                logger.exception('close environment error')

atexit.register(close_environments_atexit)

def make_namespace_key(storage_path: str, namespace: str) -> str:
    return ':'.join([storage_path, namespace.replace('.', '/')])

def get_namespace_size(
    storage_path: str,
    namespace: str,
) -> int:
    _, env, _, _, _, _ = get_environment_threadsafe(storage_path, namespace)
    return env.info()['map_size']

def set_namespace_size(
    size: int,
    storage_path: str,
    namespace: str
):
    assert size > 0
    _, env, _, _, _, _ = get_environment_threadsafe(storage_path, namespace)
    lock = filelock.FileLock(getenv(constants.GLOBAL_FILE_LOCK_PATH_ENVNAME, str))
    with lock:
        env.set_mapsize(size)

@functools.lru_cache(None)
def get_environment_threadsafe(
    storage_path: str,
    namespace: str,
    /, *,
    create: bool = True
) -> Tuple[
    str, lmdb.Environment, lmdb._Database, lmdb._Database,
    lmdb._Database, lmdb._Database
]:
    namespace_key = make_namespace_key(storage_path, namespace)

    if namespace_key not in environments:

        with environments_lock:

            if namespace_key not in environments:

                env_path = os.path.join(
                    storage_path,
                    *namespace.split('/')
                )

                if os.path.exists(env_path):
                    if not os.path.isdir(env_path):
                        raise StoragePathError()
                else:
                    try:
                        os.makedirs(env_path)
                    except FileExistsError:
                        pass
                    except OSError as exc:
                        raise StoragePathError() from exc

                if namespace.split('/')[0] == constants.MEMORY_NAMESPACE:
                    profile = get_lmdb_profiles()['memory']
                else:
                    profile = get_lmdb_profiles()['default']

                env = lmdb.open(
                    env_path, subdir = True, create = create,
                    writemap = profile['LMDB_WRITE_MAP'],
                    metasync = profile['LMDB_METASYNC'],
                    map_size = profile['LMDB_INITIAL_MAP_SIZE'],
                    map_async = profile['LMDB_MAP_ASYNC'],
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

                try:
                    txn = None
                    txn = env.begin(write = True, buffers = False)
                    env_uuid = txn.get(key = constants.ENVIRONMENT_UUID_KEY.encode('utf-8'))
                    if env_uuid is None:
                        env_uuid = str(uuid.uuid4())
                        assert txn.put(
                            key = constants.ENVIRONMENT_UUID_KEY.encode('utf-8'),
                            value = env_uuid.encode('utf-8')
                        )
                    else:
                        env_uuid = env_uuid.decode('utf-8')
                    txn.commit()
                except BaseException as exc:
                    if txn:
                        txn.abort()
                    raise TransactionError() from exc

                environments[namespace_key] = \
                (env_uuid, env, name_db, attribute_db, version_db, descriptor_db)

    return environments[namespace_key]
