import collections.abc
import logging
import parkit.storage.threadlocal as thread
import struct
import types

from parkit.exceptions import abort
from parkit.storage.context import context
from parkit.storage.database import Database

logger = logging.getLogger(__name__)

def generic_dict_iter(obj, database_index, decode_key, decode_value, keys = True, values = False):

  def dict_iter(
    self, transaction = False, zerocopy = False,
    decode_key = decode_key, decode_value = decode_value
  ):
    with context(self._lmdb, write = transaction, inherit = True, zerocopy = zerocopy):
      cursor = None
      db = self._databases[database_index]
      cursor = thread.local.cursors[db[0]]
      if not cursor.first():
        return
      while True:
        if keys and not values:
          yield decode_key(cursor.key())
        elif values and not keys:
          yield decode_value(cursor.value())
        else:
          yield (decode_key(cursor.key()), decode_value(cursor.value()))
        if not cursor.next():
          return
      
  return types.MethodType(dict_iter, obj)

def generic_dict_get(obj, database_index, encode_key, decode_value):
  
  def dict_get(
    self, key, default = None, 
    encode_key = encode_key, decode_value = decode_value
  ):
    try:
      txn = None
      key = encode_key(key) 
      db = self._databases[database_index][1]
      if not thread.local.transaction:
        txn = self._lmdb.begin()
      else:
        txn = thread.local.transaction
      result = txn.get(key = key, default = default, db = db)
      if not thread.local.transaction: txn.commit()
      return decode_value(result) if result != default else result
    except BaseException as e:
      if txn: txn.abort()
      abort(e)

  return types.MethodType(dict_get, obj)

def generic_dict_setdefault(obj, database_index, encode_key, encode_value, decode_value):
  
  def dict_setdefault(
    self, key, default = None, encode_key = encode_key, 
    encode_value = encode_value, decode_value = decode_value
  ):
    try:
      txn = cursor = None
      key = encode_key(key)
      default = encode_value(default)
      db = self._databases[database_index]
      if not thread.local.transaction:
        txn = self._lmdb.begin(write = True)
        cursor = txn.cursor(db = db[1])
      else:
        txn = thread.local.transaction
        cursor = thread.local.cursors[db[0]]
      result = cursor.set_key(key)
      if result:
        value = cursor.value()
      else:
        assert cursor.put(key = key, value = default)
        if not thread.local.transaction: 
          if self._versioned: self.increment_version(implicit_transaction = txn)
        else:
          if self._versioned: thread.local.changed.add(self)
      if not thread.local.transaction: txn.commit()
      return default if not result else decode_value(value)
    except BaseException as e:
      if txn: txn.abort()
      abort(e)
    finally:
      if not thread.local.transaction and cursor: cursor.close()

  return types.MethodType(dict_setdefault, obj)

def generic_dict_popitem(obj, database_index, decode_key, decode_value):
  
  def dict_popitem(
    self, decode_key = decode_key, decode_value = decode_value
  ):
    try:
      txn = cursor = None
      db = self._databases[database_index]
      if not thread.local.transaction:
        txn = self._lmdb.begin(write = True)
        cursor = txn.cursor(db = db[1])
      else:
        txn = thread.local.transaction
        cursor = thread.local.cursors[db[0]]
      result = cursor.last()
      if result:
        key = cursor.key()
        value = cursor.pop(key) 
        if not thread.local.transaction: 
          if self._versioned: self.increment_version(implicit_transaction = txn)
        else:
          if self._versioned: thread.local.changed.add(self)
      if not thread.local.transaction: txn.commit()
      if not result:
        raise KeyError()
      else:
        return (decode_key(key), decode_value(value))
    except BaseException as e:
      if txn: txn.abort()
      abort(e)
    finally:
      if not thread.local.transaction and cursor: cursor.close()

  return types.MethodType(dict_popitem, obj)

def generic_dict_pop(obj, database_index, encode_key, decode_value):
  
  class Unspecified():
    pass

  def dict_pop(
    self, key, default = Unspecified(),  
    encode_key = encode_key, decode_value = decode_value
  ):
    try:
      txn = cursor = None
      key = encode_key(key)
      db = self._databases[database_index]
      if not thread.local.transaction:
        txn = self._lmdb.begin(write = True)
        cursor = txn.cursor(db = db[1])
      else:
        txn = thread.local.transaction
        cursor = thread.local.cursors[db[0]]
      result = cursor.pop(key)
      if not thread.local.transaction: 
        if result is not None and self._versioned: self.increment_version(implicit_transaction = txn)
        txn.commit()
      else:
        if result is not None and self._versioned: thread.local.changed.add(self)
      if result is None and isinstance(default, Unspecified):
        raise KeyError()
      elif result is None:
        return default
      else:
        return decode_value(result)
    except BaseException as e:
      if txn: txn.abort()
      abort(e)
    finally:
      if not thread.local.transaction and cursor: cursor.close()

  return types.MethodType(dict_pop, obj)

def generic_dict_delete(obj, database_index, encode_key):
  
  def dict_delete(self, key, encode_key = encode_key):
    try:
      txn = None
      key = encode_key(key)
      db = self._databases[database_index][1]
      if not thread.local.transaction:
        txn = self._lmdb.begin(write = True)
      else:
        txn = thread.local.transaction
      result = txn.delete(key = key, db = db)
      if not thread.local.transaction: 
        if result and self._versioned: self.increment_version(implicit_transaction = txn)
        txn.commit()
      else:
        if result and self._versioned: thread.local.changed.add(self)
      return result
    except BaseException as e:
      if txn: txn.abort()
      abort(e)

  return types.MethodType(dict_delete, obj)

def generic_dict_contains(obj, database_index, encode_key):
  
  def dict_contains(self, key, encode_key = encode_key):
    try:
      txn = cursor = None
      key = encode_key(key)
      db = self._databases[database_index]
      if not thread.local.transaction:
        txn = self._lmdb.begin()
        cursor = txn.cursor(db = db[1])
      else:
        txn = thread.local.transaction
        cursor = thread.local.cursors[db[0]]
      result = cursor.set_key(key)
      if not thread.local.transaction: txn.commit()
      return result
    except BaseException as e:
      if txn: txn.abort()
      abort(e)
    finally:
      if not thread.local.transaction and cursor: cursor.close()

  return types.MethodType(dict_contains, obj)

def generic_dict_put(obj, database_index, encode_key, encode_value):
  
  def dict_put(self, key, value, encode_key = encode_key, encode_value = encode_value):
    try:
      txn = None
      key = encode_key(key) 
      value = encode_value(value)
      db = self._databases[database_index][1]
      if not thread.local.transaction:
        txn = self._lmdb.begin(write = True)
      else:
        txn = thread.local.transaction 
      result = txn.put(key = key, value = value, overwrite = True, append = False, db = db)
      if not thread.local.transaction: 
        if result and self._versioned: self.increment_version(implicit_transaction = txn)
        txn.commit()
      else:
        if result and self._versioned: thread.local.changed.add(self)
      return result
    except BaseException as e:
      if txn: txn.abort()
      abort(e)

  return types.MethodType(dict_put, obj)

def generic_dict_update(obj, database_index, encode_key, encode_value):
  
  def dict_update(self, *args, encode_key = encode_key, encode_value = encode_value, **kwargs):
    try:
      txn = cursor = dict_items = iter_items = None
      consumed = added = 0
      if len(args) and isinstance(args[0], dict):
        dict_items = [
          (encode_key(key), encode_value(value))
          for key, value in args[0].items()
        ]
      if len(args) and not dict_items and issubclass(type(args[0]), collections.abc.MutableMapping):
        iter_items = [
          (encode_key(key), encode_value(value))
          for key, value in args[0].items()
        ]
      elif len(args) and not dict_items:
        iter_items = [
          (encode_key(key), encode_value(value))
          for key, value in args[0]
        ] 
      kwargs_items = [
        (encode_key(key), encode_value(value))
        for key, value in kwargs.items()
      ]
      db = self._databases[database_index]
      if not thread.local.transaction:
        txn = self._lmdb.begin(write = True)
        cursor = txn.cursor(db = db[1])
      else:
        txn = thread.local.transaction
        cursor = thread.local.cursors[db[0]] 
      if dict_items:
        c, a = cursor.putmulti(dict_items)
        consumed += c
        added += a
      if len(kwargs_items):
        c, a = cursor.putmulti(kwargs_items)
        consumed += c
        added += a       
      if iter_items:
        c, a = cursor.putmulti(iter_items)
        consumed += c
        added += a
      if not thread.local.transaction: 
        if added and self._versioned: self.increment_version(implicit_transaction = txn)
        txn.commit()
      else:
        if added and self._versioned: thread.local.changed.add(self)
    except BaseException as e:
      if txn: txn.abort()
      abort(e)
    finally:
      if not thread.local.transaction and cursor: cursor.close()

  return types.MethodType(dict_update, obj)


  