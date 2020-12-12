import collections
import enum
import logging
import os
import struct
import threading

from parkit.constants import *
from parkit.exceptions import *
from parkit.storage.dbenv import (
  Environment,
  TransactionMode
)
from parkit.utility import *

from functools import lru_cache

logger = logging.getLogger(__name__)

database_cache = dict()

engine_cache = dict()

def get_storage_engine(
  install_path, context = None, db_name = None, integer_keys = False, versioned = False
):
  if (install_path, context, db_name, integer_keys, versioned) not in engine_cache:
    engine = StorageEngine(
      install_path,
      context = context, 
      db_name = db_name, integer_keys = integer_keys,
      versioned = versioned
    )
    engine_cache[(install_path, context, db_name, integer_keys, versioned)] = engine
  else:
    engine = engine_cache[(install_path, context, db_name, integer_keys, versioned)]

  if engine not in database_cache:
    database_cache[engine] = engine.environment.get_database(engine.encoded_db_name, engine.integer_keys)
  
  return engine

def get_database(engine):
  return database_cache[engine]

def invalidate_database(engine):
  del database_cache[engine]

class StorageEngine():

  class Executor():

    def __init__(self, mode, engine, txn_context = None):
      self._engine = engine
      self._mode = mode
      if txn_context is None:
        try:
          self._changed = False
          self._database = self._engine.environment.get_database(
            self._engine.encoded_db_name, self._engine.integer_keys
          )
          self._implicit, self._txn = (
            True,
            self._engine.environment.get_transaction(mode = self._mode)
          )
        except Exception as e:
          log_and_raise(e)
      else:
        self._txn_context = txn_context
        self._implicit, self._txn = (
          False,
          self._txn_context.get_transaction()
        )

    def set_changed(self, value):
      if self._implicit:
        self._changed = value
      else:
        self._txn_context.set_changed(self._engine, value)

    def get_database(self):
      if self._implicit:
        return self._database
      else:
        return self._txn_context.get_database(self._engine)

    @lru_cache(None)
    def get_cursor(self):
      if self._implicit:
        return self._txn.cursor(db = self._database)
      else:
        return self._txn_context.get_cursor(self._engine)

    def __enter__(self):
      return (self, self._txn)
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
      if exc_value is None:
        if self._implicit:
          if self._changed and self._engine.versioned:
            try:
              db = self._engine.environment.get_version_database()
              result = self._txn.get(self._engine.encoded_db_name, db = db)
              version = 0 if result is None else struct.unpack('@N', result)[0]
              self._txn.put(self._engine.encoded_db_name, struct.pack('@N', version + 1), db = db)
            except Exception as e:
              self._txn.abort()
              log_and_raise(e, TransactionAborted)
          self._txn.commit()
      else:
        if self._implicit:
          self._txn.abort()
          log_and_raise(exc_value, TransactionAborted)
        else:
          raise exc_value

  def __init__(
    self, install_path, context = None, db_name = None, integer_keys = False, 
    versioned = False
  ):
    self._encoded_db_name = db_name.encode('utf-8')
    self._db_name = db_name
    self._integer_keys = integer_keys
    self._versioned = versioned
    self._env = Environment(install_path, context)
    self._id = create_string_digest(
      self._env.install_path, self._env.context, self._db_name, self._integer_keys, self._versioned
    )
    self._db = self._env.get_database(self._encoded_db_name, self._integer_keys)
    self._txn = None

  @property
  def database(self):
    return self._db

  @property
  def id(self):
    return self._id

  @property
  def base_engine(self):
    return self

  @property
  def integer_keys(self):
    return self._integer_keys

  @property
  def versioned(self):
    return self._versioned

  @property
  def environment(self):
    return self._env

  @property
  def db_name(self):
    return self._db_name

  @property
  def encoded_db_name(self):
    return self._encoded_db_name

  def __hash__(self):
    return self._id.__hash__()

  def __eq__(self, other):
    return self._id == other._id

  def __getstate__(self):
    self._env = None
    return self.__dict__

  def __setstate__(self, from_wire):
    env = Environment(from_wire['_install_path'], from_wire['_context'])
    local = dict(
      _env = env,
    )
    self.__dict__ = {**from_wire, **local}

  def clear_context(self):
    self._txn = None
    self._cursor = None

  def set_context(self, txn):
    self._txn = txn
    self._cursor = txn.cursor(db = self._db)

  def version(self, txn_context = None):
    if self._versioned:
      with StorageEngine.Executor(TransactionMode.Reader, self, txn_context = txn_context) as (ctx, txn):
        result = txn.get(self._encoded_db_name, db = self._env.get_version_database())
        return 0 if result is None else struct.unpack('@N', result)[0]
    else:
      return None

  def delete(self, key, txn_context = None):
    with StorageEngine.Executor(TransactionMode.Writer, self, txn_context = txn_context) as (ctx, txn):
      result = txn.delete(key, db = ctx.get_database())
      if result:
        ctx.set_changed(True)
      return result

  def clear(self, txn_context = None):
    with StorageEngine.Executor(TransactionMode.Writer, self, txn_context = txn_context) as (ctx, txn):
      txn.drop(ctx.get_database(), delete = False)
      ctx.set_changed(True)
      return None
    
  def drop(self, txn_context = None):
    with StorageEngine.Executor(TransactionMode.Writer, self, txn_context = txn_context) as (ctx, txn):
      txn.drop(ctx.get_database(), delete = True)
      txn.delete(self._encoded_db_name, db = self._env.get_version_database())
      invalidate_database(self)
      ctx.set_changed(False)
      return None

  def contains(self, key, txn_context = None):
    with StorageEngine.Executor(TransactionMode.Reader, self, txn_context = txn_context) as (ctx, txn):
      return ctx.get_cursor().set_key(key)
  
  def append(self, value, txn_context = None):
    if not self._integer_keys:
      raise InvalidOperation()
    if self._txn is None:
      txn = self._env.get_transaction()
      db = self._db
      cursor = txn.cursor(db = db)
    else:
      txn = self._txn
      db = self._db
      cursor = self._cursor
    #with StorageEngine.Executor(TransactionMode.Writer, self, txn_context = txn_context) as (ctx, txn):
    if not cursor.last():
      key = struct.pack('@N', 0)
    else:
      key = struct.pack('@N', struct.unpack('@N', cursor.key())[0] + 1)
    result = txn.put(
      key, value, append = True, overwrite = False, 
      db = db
    )
    #if txn_context is None:
    #  txn.commit()
    #if result:
    #  ctx.set_changed(True)
    return result

  def put(self, key, value, append = False, replace = True, insert = True, txn_context = None):
    if not insert and not replace:
      return False 
    with StorageEngine.Executor(TransactionMode.Writer, self, txn_context = txn_context) as (ctx, txn):
      if not insert:
        if not ctx.get_cursor().set_key(key):
          return False
      result = txn.put(
        key, value, append = append, overwrite = replace, 
        db = ctx.get_database()
      )       
      if result:
        ctx.set_changed(True)
      return result

  def get(self, key, txn_context = None):
    with StorageEngine.Executor(TransactionMode.Reader, self, txn_context = txn_context) as (ctx, txn):
      return txn.get(key, db = ctx.get_database())
  
  def size(self, txn_context = None):
    with StorageEngine.Executor(TransactionMode.Reader, self, txn_context = txn_context) as (ctx, txn):
      return txn.stat(ctx.get_database())['entries']
  
  def keys(self, txn_context = None):
    with StorageEngine.Executor(TransactionMode.Reader, self, txn_context = txn_context) as (ctx, txn):
      return ctx.get_cursor().iternext(keys = True, values = False)
  
  def pop(self, key, txn_context = None):
    with StorageEngine.Executor(TransactionMode.Writer, self, txn_context = txn_context) as (ctx, txn):
      result = txn.pop(key, db = ctx.get_database())       
      if result:
        ctx.set_changed(True)
      return result

  def put_many(self, items, append = False, replace = True, insert = True, txn_context = None):
    if not insert and not replace:
      return False 
    with StorageEngine.Executor(TransactionMode.Writer, self, txn_context = txn_context) as (ctx, txn):
      items = [(key, value) for key, value in items]
      if not insert:
        items_removed = len(items)
        items = [(key, value) for key, value in items if ctx.get_cursor().set_key(key)]
        items_removed -= len(items)
      if len(items) == 0:
        return (0, 0)
      else:
        (consumed, added) = cursor.put_many(
          items, append = append, overwrite = replace, db = ctx.get_database()
        )
        if added:
          ctx.set_changed(True)
        return (consumed + items_removed, added)  
