import logging

from parkit.exceptions import *
from parkit.decorators.mixins import *

logger = logging.getLogger(__name__)

class TransactionInjection(DefaultSetRuntimeArgs):

  def __init__(self, txn_context):
    self._txn_context = txn_context
    
  def decorate(self, engine):
    return TransactionDecorator(engine, self._txn_context)

class TransactionDecorator(Decorator):

  def __init__(self, engine, txn_context):
    super().__init__(engine) 
    self._txn_context = txn_context
    
  @property
  def context(self):
    return self._txn_context

  def __getstate__(self):
    raise NotSerializableError()

  def clear(self, txn_context = None):
    return self.engine.clear(txn_context = self._txn_context)

  def drop(self, txn_context = None):
    return self.engine.drop(txn_context = self._txn_context)
  
  def delete(self, key, txn_context = None):
    return self.engine.delete(key, txn_context = self._txn_context)
  
  def contains(self, key, txn_context = None):
    return self.engine.contains(key, txn_context = self._txn_context)
  
  def append(self, value, txn_context = None):
    return self.engine.append(
      value, txn_context = self._txn_context
    )

  def put(self, key, value, append = False, replace = True, insert = True, txn_context = None):
    return self.engine.put(
      key, value, append = append, replace = replace, insert = insert,
      txn_context = self._txn_context
    )
  
  def get(self, key, txn_context = None):
    return self.engine.get(key, txn_context = self._txn_context)

  def version(self, txn_context = None):
    return self.engine.version(txn_context = self._txn_context)
  
  def pop(self, key, txn_context = None):
    return self.engine.pop(key, txn_context = self._txn_context)
  
  def size(self, txn_context = None):
    return self.engine.size(txn_context = self._txn_context)
  
  def keys(self, txn_context = None):
    return self.engine.keys(txn_context = self._txn_context)
  
  def put_many(self, items, append = False, replace = True, insert = True, txn_context = None):
    return self.engine.put_many(
      items, append = append, replace = replace, insert = insert,
      txn_context = self._txn_context
    )