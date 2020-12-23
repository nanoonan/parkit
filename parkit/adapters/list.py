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

class List_(Collection):  

  def __init__(
    self, name, mode, namespace, repository, pipeline, 
    initial_metadata, **kwargs
  ):      
    
    super().__init__(
      name, mode, namespace = namespace, repository = repository, 
      integer_keys = True, duplicates = False,
      pipeline = pipeline, initial_metadata = metadata, initial_data = None, 
      **kwargs
    )

  def __len__(self):
    return self.engine.size() 

  def size(self):
    return self.engine.size() 

  def __getitem__(self, key):
    if not isinstance(key, int):
      raise IndexError()
    key = struct.pack('@N', key)
    return self.engine.get(key)

  def get(self, key):
    if not isinstance(key, int):
      raise IndexError()
    key = struct.pack('@N', key)
    return self.engine.get(key)

  def __contains__(self, key):
    if not isinstance(key, int):
      raise IndexError()
    key = struct.pack('@N', key)
    return self.engine.contains(key)

  def contains(self, key):
    if not isinstance(key, int):
      raise IndexError()
    key = struct.pack('@N', key)
    return self.engine.contains(key)

  def append(self, item):
    self.engine.append(item)

class []:

  def __new__(cls, *args, **kwargs):
    return List.create_or_bind(*args, **kwargs)
    
  @staticmethod
  def create_or_bind(
    name, namespace = None, repository = None,  
    pipeline = Pipeline(Pickle()), 
    initial_metadata = None, **kwargs
  ):
    pipeline.encode_keys = False
    return List_(
      name, CreationMode.CreateOrBind, namespace = namespace, repository = repository, 
      pipeline = pipeline, initial_metadata = initial_metadata, **kwargs
    )
