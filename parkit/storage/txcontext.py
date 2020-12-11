import collections
import logging
import struct

from parkit.constants import *
from parkit.exceptions import *
from parkit.storage.engine import (
  get_database,
  StorageEngine
)
from parkit.storage.dbenv import TransactionMode

from parkit.utility import *

logger = logging.getLogger(__name__)

class TransactionContext():

  def __init__(self, parent, mode, create_transaction_fn):
    self._parent = parent
    self._mode = mode
    self._txn = None
    self._create_transaction_fn = create_transaction_fn
    self._databases = dict()
    self._cursors = dict()
    self._changed = collections.defaultdict(lambda: False)

  def get_transaction(self):
    return self._txn

  def __getstate__(self):
    raise NotSerializableError()

  def get_database(self, engine):
    if engine not in self._databases:
      self._databases[engine] = get_database(engine)
    return self._databases[engine]

  def set_changed(self, engine, value):
    if self._parent is None:
      self._changed[engine] = value
    else:
      self._parent._changed[engine] = value

  def get_cursor(self, engine):
    if engine not in self._cursors:
      self._cursors[engine] = self._txn.cursor(db = self.get_database(engine))
    return self._cursors[engine]

  def on_enter(self):
    raise NotImplementedError()

  def on_exit(self):
    raise NotImplementedError()

  def __enter__(self):
    self._txn = self._create_transaction_fn()
    return self.on_enter()
    
  def __exit__(self, exc_type, exc_value, exc_traceback):
    try:
      if exc_value is not None:
        self._txn.abort()
        log_and_raise(exc_value)
      else:
        try:
          if self._parent is None:
            for engine, changed in self._changed.items():
              if changed and engine.versioned:
                db = engine.environment.get_version_database()
                result = self._txn.get(engine.encoded_db_name, db = db)
                version = 0 if result is None else struct.unpack('@N', result)[0]
                self._txn.put(engine.encoded_db_name, struct.pack('@N', version + 1), db = db)
          self._txn.commit()
        except Exception as e:
          self._txn.abort()
          log_and_raise(e, TransactionAborted)
    finally:
      self.on_exit()
      