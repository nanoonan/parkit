import logging
import parkit.storage.threadlocal as thread
import struct
import types

from parkit.exceptions import abort
from parkit.storage.database import Database

def generic_clear(obj, database_index):
  
  def clear(self, database_index = database_index):
    try:
      txn = None
      db = self._databases[database_index][1]
      if not thread.local.transaction:
        txn = lmdb.begin(write = True)
      else:
        txn = thread.local.transaction
      txn.drop(db, delete = False)
      if not thread.local.transaction: 
        if self._versioned: self.increment_version(transaction = txn)
        txn.commit()
      else:
        if self._versioned: thread.local.changed.add(self)
      if not thread.local.transaction: txn.commit()
    except BaseException as e:
      if txn: txn.abort()
      abort(e)

  return types.MethodType(clear, obj)

def generic_size(obj, database_index):
  
  def size(self, database_index = database_index):
    try:
      txn = None
      db = self._databases[database_index][1]
      if not thread.local.transaction:
        txn = self._lmdb.begin()
      else:
        txn = thread.local.transaction
      result = txn.stat(db)['entries']
      if not thread.local.transaction: txn.commit()
      return result
    except BaseException as e:
      if txn: txn.abort()
      abort(e)

  return types.MethodType(size, obj)
