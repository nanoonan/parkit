import atexit
import distutils
import enum
import lmdb
import logging
import os
import threading

from parkit.constants import *
from parkit.exceptions import *
from parkit.utility import *

from functools import lru_cache

logger = logging.getLogger(__name__)

lock = threading.Lock()
environments = dict()

class TransactionMode(enum.Enum):
  Reader = 1
  Writer = 2

def close_environments_atexit():
  with lock:
    for env in environments.values():
      try:
        env.close()
      except Exception as e:
        log(e)

atexit.register(close_environments_atexit)

@lru_cache(None)
def get_environment(install_path, context):
  if (install_path, context) not in environments:
    with lock:
      if (install_path, context) not in environments:
        env_path = os.path.join(install_path, context)
        if os.path.exists(env_path):
          if not os.path.isdir(env_path):
            raise InvalidPath()
        env = lmdb.open(
          env_path, subdir = True, create = True,
          writemap = bool(distutils.util.strtobool(getenv(LMDB_WRITE_MAP_ENVNAME))),
          map_async = bool(distutils.util.strtobool(getenv(LMDB_MAP_ASYNC_ENVNAME))), 
          map_size = int(getenv(LMDB_MAP_SIZE_ENVNAME)), 
          max_dbs = int(getenv(LMDB_MAX_DBS_ENVNAME)),
          max_spare_txns = int(getenv(LMDB_MAX_SPARE_TXNS_ENVNAME)),
          max_readers = int(getenv(LMDB_MAX_READERS_ENVNAME)),
          readonly = bool(distutils.util.strtobool(getenv(LMDB_READONLY_ENVNAME))), 
          sync = bool(distutils.util.strtobool(getenv(LMDB_SYNC_ENVNAME))),
          meminit = bool(distutils.util.strtobool(getenv(LMDB_MEMINIT_ENVNAME)))
        )
        version_db = env.open_db(
          key = VERSION_DATABASE_NAME.encode('utf-8'), integerkey = False
        )
        environments[(install_path, context)] = (env, version_db)
  return environments[(install_path, context)]

class Environment():

  def __init__(self, install_path, context):
    self._context = context
    self._install_path = install_path
    self._env, self._version_db = get_environment(self._install_path, self._context)
    
  @property
  def install_path(self):
    return self._install_path

  @property
  def context(self):
    return self._context
    
  def __getstate__(self):
    raise NotSerializableError()

  def get_transaction(self, mode = TransactionMode.Writer, parent = None):
    return self._env.begin(
      db = None, write = False if mode == TransactionMode.Reader else True, 
      parent = parent, buffers = True
    )

  def get_version_database(self):
    return self._version_db

  def get_database(self, encoded_db_name, integer_keys = False):
    return self._env.open_db(key = encoded_db_name, integerkey = integer_keys)
    
  
