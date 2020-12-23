import logging
import struct

from parkit.adapters.collection import Collection
from parkit.adapters.encoders import Pickle
from parkit.adapters.mixins import (
  generic_queue_get,
  generic_queue_put,
  generic_size
)
from parkit.constants import *
from parkit.exceptions import *
from parkit.utility import *
from parkit.storage import context

logger = logging.getLogger(__name__)

class Queue(Collection):  

  def __init__(
    self, name, namespace = None, repository = None, create = False, bind = True, 
    encoders = [Pickle], initial_metadata = None, initializer = None
  ):      
    
    super().__init__(name, namespace = namespace, repository = repository)

    self._mixin_methods()

    kwargs = dict(
      request_databases = [dict(integer_keys = True, duplicates = False)],
      request_versioned = False, 
      create = create, 
      bind = bind,
      encoders = encoders, 
      initial_metadata = initial_metadata
    )

    self._pre_create_or_bind(kwargs)

    with context(self, write = create, inherit = True) as txn:  
      self._create_or_bind(txn, kwargs)
      if initializer is not None:
        initializer()

  def _mixin_methods(self):
    if not hasattr(Queue, '__len__'):
      setattr(Queue, '__len__', generic_size(self))
    self.qsize = generic_size(self)
    self.get = generic_queue_get(self, fifo = True)
    self.put = generic_queue_put(self)

  def __getstate__(self):
    to_wire = super().__getstate__()
    del to_wire['qsize']
    del to_wire['get']
    del to_wire['put']
    return to_wire

  def __setstate__(self, from_wire):
    super().__setstate__(from_wire)
    self._mixin_methods()

  def empty(self):
    return self.qsize() == 0

  
      
