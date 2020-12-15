import logging

logger = logging.getLogger(__name__)

class DefaultSetRuntimeArgs():

  def set_runtime_args(self, **kwargs):
    pass

class DefaultClear():

  def clear(self):
    return self._engine.clear()

class DefaultDrop():

  def drop(self):
    return self._engine.drop()

class DefaultDelete():
  
  def delete(self, key = None, first = True):
    return self._engine.delete(key)

class DefaultContains():
  
  def contains(self, key):
    return self._engine.contains(key)

class DefaultPut():
  
  def put(self, key, value, replace = True, insert = True):
    return self._engine.put(
      key, value, replace = replace, insert = insert,
      txn_context = txn_context
    )

class DefaultAppend():
  
  def put(self, value, key = None):
    return self._engine.append(
      value, key = key
    )

class DefaultGet():
  
  def get(self, key = None, first = True, default = None):
    return self._engine.get(key = key, first = first, default = default)

class DefaultGetVersion():
  
  def get_version(self):
    return self._engine.get_version()

class DefaultGetMetadata():
  
  def get_metadata(self):
    return self._engine.get_metadata()

class DefaultPutMetadata():
  
  def put_metadata(self, item):
    return self._engine.put_metadata(item)

class DefaultPop():
  
  def pop(self, key = None, first = True, default = None):
    return self._engine.pop(key = key, first = first, default = default)

class DefaultSize():
  
  def size(self):
    return self._engine.size()

class DefaultKeys():
  
  def keys(self):
    return self._engine.keys()

class DefaultPutMany():
  
  def put_many(self, items, replace = True, insert = True):
    return self._engine.put_many(
      items, replace = replace, insert = insert,
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
  def storage_engine(self):
    return self._storage_engine
  