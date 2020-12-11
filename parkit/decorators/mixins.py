import logging

from parkit.storage import TransactionMode

logger = logging.getLogger(__name__)

class DefaultSetRuntimeArgs():

  def set_runtime_args(self, **kwargs):
    pass

class DefaultClear():

  def clear(self, txn_context = None):
    return self._engine.clear(txn_context = txn_context)

class DefaultDrop():

  def drop(self, txn_context = None):
    return self._engine.drop(txn_context = txn_context)

class DefaultDelete():
  
  def delete(self, key, txn_context = None):
    return self._engine.delete(key, txn_context = txn_context)

class DefaultContains():
  
  def contains(self, key, txn_context = None):
    return self._engine.contains(key, txn_context = txn_context)

class DefaultPut():
  
  def put(self, key, value, append = False, replace = True, insert = True, txn_context = None):
    return self._engine.put(
      key, value, append = append, replace = replace, insert = insert,
      txn_context = txn_context
    )

class DefaultAppend():
  
  def put(self, value, txn_context = None):
    return self._engine.append(
      value, txn_context = txn_context
    )

class DefaultGet():
  
  def get(self, key, txn_context = None):
    return self._engine.get(key, txn_context = txn_context)

class DefaultVersion():
  
  def version(self, txn_context = None):
    return self._engine.version(txn_context = txn_context)

class DefaultPop():
  
  def pop(self, key, txn_context = None):
    return self._engine.pop(key, txn_context = txn_context)

class DefaultSize():
  
  def size(self, txn_context = None):
    return self._engine.size(txn_context = txn_context)

class DefaultKeys():
  
  def keys(self, txn_context = None):
    return self._engine.keys(txn_context = txn_context)

class DefaultPutMany():
  
  def put_many(self, items, append = False, replace = True, insert = True, txn_context = None):
    return self._engine.put_many(
      items, append = append, replace = replace, insert = insert,
      txn_context = txn_context
    )

class Decorator():

  def __init__(self, engine):
    self._engine = engine
    self._base_engine = engine.base_engine

  @property
  def engine(self):
    return self._engine

  @property
  def base_engine(self):
    return self._base_engine
  