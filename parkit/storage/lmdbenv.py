import atexit
import datetime
import distutils.util
import enum
import orjson
import lmdb
import logging
import os
import struct
import threading
import uuid

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
    for env, _, _, _ in environments.values():
      try:
        env.close()
      except Exception as e:
        log(e)

atexit.register(close_environments_atexit)

def normalize_repository(repository):
  if os.path.exists(repository):
    if not os.path.isdir(repository):
      raise InvalidPath()
  return os.path.abspath(repository)

def normalize_namespace(namespace):
  return create_id(namespace)

@lru_cache(None)
def get_environment(repository, namespace):
  if (repository, namespace) not in environments:
    with lock:
      if (repository, namespace) not in environments:
        env_path = os.path.join(repository, namespace)
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
        instance_db = env.open_db(
          key = INSTANCE_DATABASE_NAME.encode('utf-8'), integerkey = False
        ) 
        version_db = env.open_db(
          key = VERSION_DATABASE_NAME.encode('utf-8'), integerkey = False
        )
        metadata_db = env.open_db(
          key = METADATA_DATABASE_NAME.encode('utf-8'), integerkey = False
        )
        environments[(repository, namespace)] = (env, metadata_db, version_db, instance_db)
  return environments[(repository, namespace)]

class LMDBEnvironment():

  __json__ = True

  def __init__(self, repository, namespace = None):
    self._qualified_class_name = get_qualified_class_name(self)
    self._namespace = normalize_namespace(namespace if namespace is not None else DEFAULT_NAMESPACE).encode('utf-8')
    self._repository = normalize_repository(repository).encode('utf-8')
    self._environment, self._metadata_database, self._version_database, self._instance_database = \
    get_environment(self._repository, self._namespace)
    
  @property
  def qualified_class_name(self):
    return self._qualified_class_name

  @property
  def repository(self):
    return self._repository.decode()

  @property
  def namespace(self):
    return self._namespace.decode()

  def __getstate__(self):
    to_wire = dict(self.__dict__)
    del to_wire['_environment'] 
    del to_wire['_version_database']
    del to_wire['_instance_database']
    del to_wire['_metadata_database']
    return to_wire

  def __setstate__(self, from_wire):
    environment, metadata_database, version_database, instance_database = \
    get_environment(from_wire['_repository'], from_wire['_namespace'])
    local = dict(
      _environment = environment,
      _version_database = version_database,
      _instance_database = instance_database,
      _metadata_database = metadata_database
    )
    self.__dict__ = {**from_wire, **local}

  def _open_database(self, txn, encoded_name, properties):
    if properties['integer_keys']:
      if properties['duplicates']:
        return self._environment.open_db(
          txn = txn, key = encoded_name, integerkey = True, dupsort = True, 
          dupfixed = True, integerdup = True, create = True
        )
      else:
        return self._environment.open_db(
          txn = txn, key = encoded_name, integerkey = True, dupsort = False,
          dupfixed = False, integerdup = False, create = True
        )
    else: 
      return self._environment.open_db(
        txn = txn, key = encoded_name, integerkey = False, 
        dupsort = properties['duplicates'], create = True
      )

  def _get_instances(self, txn, encoded_name, uuid):
    db_uuid = txn.get(key = encoded_name, db = self._instance_database)
    if not db_uuid == uuid:
        raise ObjectDropped()
    metadata = orjson.loads(bytes(txn.get(key = uuid, db = self._metadata_database)))
    instances = []
    for name, properties in metadata['instances']:
      instances.append(self._open_database(txn, name.encode('utf-8'), properties))
    return instances
    
  def _create_metadata(self, encoded_name, request_instances, request_versioned = True):
    metadata = dict(
      instances = list(zip([((encoded_name + str(i).encode('utf-8')).decode()) for i in range(len(request_instances))], request_instances)),
      versioned = request_versioned,
      created = datetime.datetime.now()
    )
    return (metadata, orjson.dumps(metadata))

  def _create_or_bind_database(
    self, txn, encoded_name, metadata = None, packed_metadata = None, create = True
  ):
    db_uuid = txn.get(key = encoded_name, db = self._instance_database)
    if db_uuid is not None:
      metadata = orjson.loads(bytes(txn.get(key = db_uuid, db = self._metadata_database)))
      instances = []
      for name, properties in metadata['instances']:
        instances.append(self._open_database(txn, name.encode('utf-8'), properties))
      return (False, metadata, bytes(db_uuid), instances)
    else:
      if not create:
        raise ObjectNotFound()
      db_uuid = uuid.uuid4().bytes
      txn.put(key = encoded_name, value = db_uuid, db = self._instance_database)
      txn.put(key = db_uuid, value = packed_metadata, db = self._metadata_database)      
      if metadata['versioned']:
        txn.put(key = db_uuid, value = struct.pack('@N', 0), db = self._version_database) 
      instances = []
      for name, properties in metadata['instances']:
        instances.append(self._open_database(txn, name.encode('utf-8'), properties))
      return (True, metadata, db_uuid, instances)
    
    
  
