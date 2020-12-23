import logging
import orjson
import parkit.constants as constants
import parkit.storage.threadlocal as thread
import struct
import uuid

from parkit.exceptions import (
  abort,
  ObjectNotFoundError
)
from parkit.storage.context import context
from parkit.storage.database import Database

logger = logging.getLogger(__name__)

class LMDBPublic():

  @property
  def path(self):
    return '/'.join([self._namespace, self._encoded_name.decode('utf-8')])

  @property
  def uuid(self):
    if self._uuid_bytes:
      return str(uuid.UUID(bytes = self._uuid_bytes))
    else:
      return None

  @property
  def versioned(self):
    return self._versioned

  @property
  def persistent(self):
    return self.path.startswith(constants.PERSISTENT_NAMESPACE)

  @property
  def creator(self):
    return self._creator

  @property
  def namespace(self):
    return self._namespace 

  @property
  def name(self):
    return self._encoded_name.decode('utf-8')

  def on_unbind(self):
    pass

  def on_bind(self):
    pass 

  def increment_version(self, implicit_transaction = None):
    if not self._versioned:
      return 
    try:
      txn = cursor = None
      db = self._databases[Database.Version.value]
      if implicit_transaction:
        assert not thread.local.transaction 
        txn = implicit_transaction
        cursor = txn.cursor(db = db[1])
      elif not thread.local.transaction:
        txn = self._lmdb.begin(write = True)
        cursor = txn.cursor(db = db[1])
      else:
        txn = thread.local.transaction
        cursor = thread.local.cursors[db[0]]
      if cursor.set_key(self._uuid_bytes):
        version = struct.pack('@N', struct.unpack('@N', cursor.value())[0] + 1)
        assert cursor.put(key = self._uuid_bytes, value = version)
      else:
        raise ObjectNotFoundError()
      if not thread.local.transaction and not implicit_transaction: txn.commit()
    except BaseException as e:
      if txn: txn.abort()
      abort(e)
    finally:
      if (implicit_transaction or not thread.local.transaction) and cursor: cursor.close()

  @property
  def exists(self):
    try:
      txn = None
      db = self._databases[Database.Name.value][1]
      if not thread.local.transaction:
        txn = self._lmdb.begin()
      else:
        txn = thread.local.transaction
      obj_uuid = txn.get(key = self._encoded_name, db = db)
      result = obj_uuid == self._uuid_bytes
      if not thread.local.transaction: txn.commit()
      return result
    except BaseException as e:
      if txn: txn.abort()
      abort(e)
  
  @property
  def version(self):
    try:
      txn = cursor = None
      db = self._databases[Database.Version.value]
      if not thread.local.transaction:
        txn = self._lmdb.begin()
        cursor = txn.cursor(db = db[1])
      else:
        txn = thread.local.transaction
        cursor = thread.local.cursors[db[0]]
      if cursor.set_key(self._uuid_bytes):
        version = cursor.value()
        version = struct.unpack('@N', version)[0] 
      else:
        raise ObjectNotFoundError()
      if not thread.local.transaction: txn.commit()
      return version
    except BaseException as e:
      if txn: txn.abort()
      abort(e)
    finally:
      if not thread.local.transaction and cursor: cursor.close()

  def drop(self):
    try:
      txn = None
      name_db = self._databases[Database.Name.value][1]
      version_db = self._databases[Database.Version.value][1]
      metadata_db = self._databases[Database.Metadata.value][1]
      if not thread.local.transaction:
        txn = self._lmdb.begin(write = True)
      else:
        txn = thread.local.transaction
      obj_uuid = txn.get(key = self._encoded_name, db = name_db)
      if obj_uuid == self._uuid_bytes:
        [txn.drop(db, delete = True) for _, db in self._databases[Database.First.value:]]
        assert txn.delete(key = self._encoded_name, db = name_db)
        assert txn.delete(key = self._uuid_bytes, db = metadata_db)
        assert txn.delete(key = self._uuid_bytes, db = version_db)
      if not thread.local.transaction: txn.commit()
    except BaseException as e:
      if txn: txn.abort()
      abort(e)

  @property
  def metadata(self):
    try:
      txn = cursor = None
      db = self._databases[Database.Metadata.value]
      if not thread.local.transaction:
        txn = self._lmdb.begin()
        cursor = txn.cursor(db = db[1])
      else:
        txn = thread.local.transaction
        cursor = thread.local.cursors[db[0]]
      if cursor.set_key(self._uuid_bytes):
        item = bytes(cursor.value())
      else:
        raise ObjectNotFoundError()
      if not thread.local.transaction: txn.commit()
      return orjson.loads(item) 
    except BaseException as e:
      if txn: txn.abort()
      abort(e)
    finally:
      if not thread.local.transaction and cursor: cursor.close()

  def update_metadata(self, updater = lambda metadata: metadata):
    with context(self._lmdb, write = True, inherit = True):
      metadata = self._get_metadata()
      metadata['user'] = updater(metadata['user'])
      db = self._databases[Database.Metadata.value]
      cursor = thread.local.cursors[db[0]]
      assert cursor.set_key(self._uuid_bytes)
      assert cursor.put(key = self._uuid_bytes, value = orjson.dumps(metadata))
  
      