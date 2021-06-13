# pylint: disable = no-member, protected-access
import atexit
import functools
import logging
import os
import struct
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

environment_lock: threading.Lock = threading.Lock()

environment: Dict[
    str,
    Tuple[
        str, lmdb.Environment, lmdb._Database, lmdb._Database,
        lmdb._Database, lmdb._Database
    ]
] = {}

mapsize: Dict[str, int] = {}

def close_environment_atexit():
    with environment_lock:
        for _, env, _, _, _, _ in environment.values():
            try:
                env.close()
            except lmdb.Error:
                logger.exception('close environment error')

atexit.register(close_environment_atexit)

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
    _, env, _, _, _, _ = get_environment_threadsafe(storage_path, namespace)
    _, site_env, _, _, _, _ = get_environment_threadsafe(
        storage_path, constants.ROOT_NAMESPACE
    )
    file_lock = filelock.FileLock(getenv(constants.GLOBAL_FILE_LOCK_PATH_ENVNAME, str))
    with environment_lock:
        with file_lock:
            env.set_mapsize(size)
            try:
                txn = None
                txn = site_env.begin(write = True, buffers = False)
                assert txn.put(
                    key = namespace.encode('utf-8'),
                    value = struct.pack('@N', size)
                )
                txn.commit()
            except BaseException as exc:
                if txn:
                    txn.abort()
                raise TransactionError() from exc
            mapsize[namespace] = size

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

    if namespace_key not in environment:

        with environment_lock:

            if namespace_key not in environment:

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
                    map_size = profile['LMDB_INITIAL_MAP_SIZE'] \
                    if namespace not in mapsize else mapsize[namespace],
                    map_async = profile['LMDB_MAP_ASYNC'],
                    max_dbs = profile['LMDB_MAX_DBS'],
                    max_spare_txns = profile['LMDB_MAX_SPARE_TXNS'],
                    max_readers = profile['LMDB_MAX_READERS'],
                    readonly = profile['LMDB_READONLY'],
                    sync = profile['LMDB_SYNC'],
                    meminit = profile['LMDB_MEMINIT']
                )

                env.reader_check()

                if namespace == constants.ROOT_NAMESPACE:
                    name_db = version_db = descriptor_db = attribute_db = None
                else:
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
                    if namespace == constants.ROOT_NAMESPACE:
                        cursor = txn.cursor()
                        cursor.first()
                        while True:
                            if cursor.key().decode('utf-8') not in [
                                constants.ENVIRONMENT_UUID_KEY,
                                constants.ATTRIBUTE_DATABASE,
                                constants.VERSION_DATABASE,
                                constants.DESCRIPTOR_DATABASE,
                                constants.NAME_DATABASE
                            ]:
                                mapsize[cursor.key().decode('utf-8')] = \
                                struct.unpack('@N', cursor.value())[0]
                            if not cursor.next():
                                break
                        txn.commit()
                except BaseException as exc:
                    if txn:
                        txn.abort()
                    raise TransactionError() from exc

                environment[namespace_key] = \
                (env_uuid, env, name_db, attribute_db, version_db, descriptor_db)

    return environment[namespace_key]
