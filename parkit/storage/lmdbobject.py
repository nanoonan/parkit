import datetime
import logging
import orjson
import os
import parkit.constants as constants
import parkit.storage.threadlocal as thread
import struct
import uuid

from parkit.storage.context import context
from parkit.storage.database import Database
from parkit.exceptions import (
  ObjectExistsError,
  ObjectNotFoundError
)
from parkit.storage.lmdbbase import LMDBBase
from parkit.storage.lmdbenv import (
  get_database,
  get_environment
)
from parkit.storage.lmdbpublic import LMDBPublic
from parkit.utility import (
  create_string_digest,
  get_qualified_class_name,
  resolve
)

logger = logging.getLogger(__name__)

class LMDBObject(LMDBBase, LMDBPublic):

  def __init__(
    self, path, create = True, bind = True, versioned = False,
    on_create = lambda: None, databases = [{}]
  ):
    if not create and not bind:
      raise ValueError()

    name, namespace = resolve(path, path = True)
    self._encoded_name = name.encode('utf-8')
    self._namespace = namespace if namespace else constants.DEFAULT_NAMESPACE
    self._lmdb, metadata_db, version_db, instance_db = get_environment(self._namespace)
    self._databases = [
      (id(instance_db), instance_db), 
      (id(metadata_db), metadata_db), 
      (id(version_db), version_db)
    ]

    is_bound = False

    if bind:
      is_bound, metadata = self._try_bind()
    
    if is_bound: 
      self._finish_bind(metadata)
      self.on_bind()
    elif create:
      with context(self._lmdb, write = True, inherit = True):    
        self._create(databases = databases, versioned = versioned)
        self.on_bind()
        on_create()
    else:
      raise ObjectNotFoundError()

  def __hash__(self):
    return self._uuid_bytes

  def __ne__(self, other):
    return not self.__eq__(other)

  def __eq__(self, other):
    if issubclass(type(other), LMDBBase):
      return self._uuid_bytes == other._uuid_bytes
    else:
      return False

  def __getstate__(self):
    to_wire = dict(self.__dict__)
    del to_wire['_lmdb'] 
    del to_wire['_databases']
    to_wire['_encoded_name'] = to_wire['_encoded_name'].decode('utf-8')
    to_wire['_uuid_bytes'] = str(uuid.UUID(bytes = self._uuid_bytes)) if to_wire['_uuid_bytes'] else None
    to_wire['_creator'] = False
    self.on_unbind()
    return to_wire

  def __setstate__(self, from_wire):
    self.__dict__ = from_wire
    self._lmdb, metadata_db, version_db, instance_db = get_environment(self._namespace)
    self._databases = [
      (id(instance_db), instance_db), 
      (id(metadata_db), metadata_db), 
      (id(version_db), version_db)
    ]
    self._encoded_name = self._encoded_name.encode('utf-8')
    self._uuid_bytes = uuid.UUID(self._uuid_bytes).bytes if self._uuid_bytes else None
    if self._uuid_bytes:
      with context(self._lmdb, write = False, inherit = True):
        txn = thread.local.transaction
        name_db = self._databases[Database.Name.value][1]
        metadata_db = self._databases[Database.Metadata.value][1]
        obj_uuid = txn.get(key = self._encoded_name, db = name_db)
        if obj_uuid != self._uuid_bytes:
            raise ObjectNotFoundError()
        metadata = orjson.loads(bytes(txn.get(key = obj_uuid, db = metadata_db)))
      self._finish_bind(metadata)
    self.on_bind()

  def _try_bind(self):
    with context(self._lmdb, write = False, inherit = True): 
      name_db = self._databases[Database.Name.value][1]
      txn = thread.local.transaction
      obj_uuid = txn.get(key = self._encoded_name, db = name_db)
      if obj_uuid:
        metadata_db = self._databases[Database.Metadata.value][1]
        metadata = orjson.loads(bytes(txn.get(key = obj_uuid, db = metadata_db)))
        if metadata['system']['type'] != get_qualified_class_name(self):
          raise TypeError()
        self._uuid_bytes = bytes(obj_uuid)
        self._creator = False
        self._versioned = metadata['system']['versioned']
        return (True, metadata)
      else:
        return (False, None)

  def _finish_bind(self, metadata):
    self._databases.extend([None] * len(metadata['system']['databases']))
    databases = [
      (
        index + len(self._databases), 
        thread.local.databases[db_uid] if db_uid in thread.local.databases else None, 
        db_uid, properties
      ) 
      for index, (db_uid, properties) in enumerate(metadata['system']['databases'])
    ]
    if any([db is None for _, db, _, _ in databases]):
      with context(self._lmdb, write = True, inherit = True):
        txn = thread.local.transaction
        for index, _, db_uid, properties in databases:
          self._databases[index] = (
            db_uid,
            get_database(txn, self._lmdb, db_uid, properties, create = False)
          )
    else:
      with context(self._lmdb, write = False, inherit = True):
        for index, db, db_uid, _ in databases:
          self._databases[index] = (db_uid, db)

  def _create(self, databases = [], versioned = False):
    name_db = self._databases[Database.Name.value][1]
    txn = thread.local.transaction
    obj_uuid = txn.get(key = self._encoded_name, db = name_db)
    if obj_uuid:
      raise ObjectExistsError()
    obj_uuid = uuid.uuid4().bytes
    version_db = self._databases[Database.Version.value][1]
    metadata_db = self._databases[Database.Metadata.value][1]
    assert txn.put(key = self._encoded_name, value = obj_uuid, db = name_db)
    assert txn.put(key = obj_uuid, value = struct.pack('@N', 0), db = version_db)
    basename = str(uuid.uuid4()) if len(databases) else None
    metadata = dict(
      system = dict(
        databases = list(
          zip(
            [
              create_string_digest(''.join([basename, str(i)])) 
              for i in range(len(databases))
            ], 
            databases
          )
        ),
        versioned = versioned,
        created = datetime.datetime.now(),
        type = get_qualified_class_name(self)
      ),
      user = {}
    )
    assert txn.put(key = obj_uuid, value = orjson.dumps(metadata), db = metadata_db)
    [
      self._databases.append((
        db_uid,
        get_database(txn, self._lmdb, db_uid, properties, create = True)
      ))
      for db_uid, properties in metadata['system']['databases']
    ]
    self._versioned = metadata['system']['versioned']
    self._uuid_bytes = obj_uuid
    self._creator = True
  
   
    
  