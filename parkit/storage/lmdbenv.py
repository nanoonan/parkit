import atexit
import distutils.util
import functools
import lmdb
import logging
import os
import parkit.constants as constants
import parkit.profiles as profiles
import parkit.storage.threadlocal as thread
import threading

from parkit.exceptions import log
from parkit.utility import getenv

from typing import (
  Dict
)

logger = logging.getLogger(__name__)

lock = threading.Lock()

environments = {}

def close_environments_atexit():
  with lock:
    for env, _, _, _ in environments.values():
      try:
        env.close()
      except Exception as e:
        log(e)

atexit.register(close_environments_atexit)

def get_database(
  transaction: lmdb.Transaction, lmdb: lmdb.Environment, database_uid: str, 
  properties: Dict[str, bool], create:  bool = False
) -> lmdb._Database:
  if database_uid in thread.local.databases:
    assert not create
    return thread.local.databases[database_uid]
  db = lmdb.open_db(
    txn = transaction, key = database_uid.encode('utf-8'), 
    integerkey = properties['integerkey'] if 'integerkey' in properties else False, 
    dupsort = properties['dupsort'] if 'dupsort' in properties else False, 
    dupfixed = properties['dupfixed'] if 'dupfixed' in properties else False, 
    integerdup = properties['integerdup'] if 'integerdup' in properties else False, 
    reverse_key = properties['reverse_key'] if 'reverse_key' in properties else False, 
    create = create
  )
  thread.local.databases[database_uid] = db
  return db

class EnvironmentDict(dict):

  def __getitem__(self, key):
    if not dict.__contains__(self, key):
      lmdb, metadata_db, version_db, name_db = _get_environment(key)
      thread.local.databases[id(metadata_db)] = metadata_db
      thread.local.databases[id(version_db)] = version_db
      thread.local.databases[id(name_db)] = name_db
      dict.__setitem__(self, key, (lmdb, metadata_db, version_db, name_db))
      return (lmdb, metadata_db, version_db, name_db)
    return dict.__getitem__(self, key)

class ThreadLocalVars(threading.local):
  
  def __init__(self):
    super().__init__()
    self.__dict__['cache'] = EnvironmentDict()

local = ThreadLocalVars()

def get_environment(namespace: str) -> (lmdb.Environment, lmdb._Database, lmdb._Database, lmdb._Database):
  return local.cache[namespace]

@functools.lru_cache
def _get_environment(namespace: str) -> (lmdb.Environment, lmdb._Database, lmdb._Database, lmdb._Database):
  if namespace not in environments:
    with lock:
      if namespace not in environments:
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
          profile = profiles.lmdb_profiles['persistent']
        else:
          profile = profiles.lmdb_profiles['volatile']
        lmdb_ = lmdb.open(
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
        name_db = lmdb_.open_db(
          key = constants.NAME_DATABASE.encode('utf-8'), integerkey = False
        ) 
        version_db = lmdb_.open_db(
          key = constants.VERSION_DATABASE.encode('utf-8'), integerkey = False
        )
        metadata_db = lmdb_.open_db(
          key = constants.METADATA_DATABASE.encode('utf-8'), integerkey = False
        )
        environments[namespace] = (lmdb_, metadata_db, version_db, name_db)
  return environments[namespace]



