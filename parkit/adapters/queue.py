import logging
import struct

from parkit.adapters.collection import (
  Collection,
  CreationMode
)
from parkit.constants import *
from parkit.exceptions import *
from parkit.storage import (
  Pickle,
  Pipeline
)
from parkit.utility import *

logger = logging.getLogger(__name__)

class NotFound():
  pass

class Queue_(Collection):  

  def __init__(self, name, mode, namespace, repository, pipeline, initial_metadata, **kwargs):      
    
    super().__init__(
      name, mode, namespace = namespace, repository = repository, 
      integer_keys = True, duplicates = False,
      pipeline = pipeline, initial_metadata = initial_metadata, initial_data = None, 
      **kwargs
    )

  def __len__(self):
    return self.engine.size() 

  def qsize(self):
    return self.engine.size() 

  def empty(self):
    return self.qsize() == 0

  def get(self, block = True, timeout = None):
    result = self.engine.pop(key = None, first = True, default = NotFound())
    if isinstance(result, NotFound):
      raise Empty()
    else:
      return result
    
  def put(self, item):
    return self.engine.append(item) 

class Queue():

  def __new__(cls, *args, **kwargs):
    return Queue.create_or_bind(*args, **kwargs)
    
  @staticmethod
  def create_or_bind(
    name, namespace = None, repository = None,  
    pipeline = Pipeline(Pickle()), 
    initial_metadata = None, **kwargs
  ):
    pipeline.encode_keys = False
    return Queue_(
      name, CreationMode.CreateOrBind, namespace = namespace, repository = repository, 
      pipeline = pipeline, initial_metadata = initial_metadata, 
      **kwargs
    )
