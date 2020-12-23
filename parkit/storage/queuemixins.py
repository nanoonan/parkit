import logging
import parkit.storage.threadlocal as thread
import struct
import types

from parkit.exceptions import abort
from parkit.storage.database import Database

logger = logging.getLogger(__name__)

def generic_queue_get(obj, database = Database.First.value, fifo = True):

  def queue_get(self):
    try:
      txn = cursor = None
      db = self._databases[database]
      if not thread.local.transaction:
        txn = self._lmdb.begin()
        cursor = txn.cursor(db = db[1])
      else:
        txn = thread.local.transaction
        cursor = thread.local.cursors[db[0]]
      exists = cursor.first() if fifo else cursor.last()
      if not exists:
        return None
      else:
        result = cursor.pop(cursor.key())
        assert result is not None
      if not thread.local.transaction:
        if self.versioned: self.increment_version()
        txn.commit()
      else:
        if self._versioned: thread.local.changed.add(self)
      return self.decode(result) if self.encode_values else result
    except BaseException as e:
      if txn: txn.abort()
      abort(e)
    finally:
      if not thread.local.transaction and cursor: cursor.close()
  
  return types.MethodType(queue_get, obj)

def generic_queue_put(obj, database = Database.First.value):

  def queue_put(self, item):
    if item is None:
      return False
    try:
      txn = cursor = None
      item = self.encode(item) if self.encode_values else item
      db = self._databases[database]
      if not thread.local.transaction:
        txn = self._lmdb.begin(write = True)
        cursor = txn.cursor(db = db[1])
      else:
        txn = thread.local.transaction
        cursor = thread.local.cursors[db[0]]
      if not cursor.last():
        key = struct.pack('@N', 0)
      else:
        key = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)
      result = cursor.put(key = key, value = item, append = True)
      if not thread.local.transaction: 
        if result and self.versioned: self.increment_version(transaction = txn)
        txn.commit()
      else:
        if result and self.versioned: thread.local.changed.add(self)      
      return result
    except BaseException as e:
      if txn: txn.abort()
      abort(e)
    finally:
      if not thread.local.transaction and cursor: cursor.close()

  return types.MethodType(queue_put, obj)