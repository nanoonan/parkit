import logging

from parkit.constants import *
from parkit.decorators import (
  TransactionDecorator,
  TransactionInjection
)
from parkit.exceptions import *
from parkit.storage import (
  TransactionMode,
  TransactionContext
)
from parkit.utility import *

logger = logging.getLogger(__name__)

class CollectionContext(TransactionContext):

  def __init__(self, mode, *collections):
    self._mode = mode
    self._collections = list(collections)
    
    self._parent = None
    reference_decorator = self._collections[0].engine
    if isinstance(reference_decorator, TransactionDecorator):
      self._parent = reference_decorator.context
    
    super().__init__(
      self._parent,
      self._mode,
      self._create_transaction
    )

  def _create_transaction(self):
    if self._parent is not None:
      parent_txn = self._parent.get_transaction()
      return self._collections[0].base_engine.environment.get_transaction(mode = self._mode, parent = parent_txn)
    else:
      return self._collections[0].base_engine.environment.get_transaction(mode = self._mode)

  def on_enter(self):
    for collection in self._collections:
      collection.push_decorator(TransactionInjection(self))
    if len(self._collections) == 1:
      return self._collections[0]
    else:
      return tuple(self._collections)

  def on_exit(self):
    for collection in self._collections:
      collection.pop_decorator()

class WriteTransaction(CollectionContext):

  def __init__(self, *collections):
    super().__init__(TransactionMode.Writer, *collections)

class ReadTransaction(CollectionContext):

  def __init__(self, *collections):
    super().__init__(TransactionMode.Reader, *collections)

class EngineContext(TransactionContext):

  def __init__(self, mode, *engines):
    self._engines = list(engines)
    super().__init__(
      None,
      mode,
      lambda: self._engines[0].base_engine.environment.get_transaction(mode = mode)
    )

  def on_enter(self):
    if len(self._engines) == 1:
      return TransactionDecorator(self._engines[0], self)
    else:
      return tuple([TransactionDecorator(engine, self) for engine in self._engines])
  
  def on_exit(self):
    pass


