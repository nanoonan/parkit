import builtins
import logging

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

class Dict(Collection):  

  def __init__(
    self, name, mode, namespace, repository, pipeline, initial_metadata, initial_data, 
    **kwargs
  ):      
    
    super().__init__(
      name, mode, namespace = namespace, repository = repository, 
      integer_keys = False, duplicates = False,
      pipeline = pipeline, initial_metadata = initial_metadata, initial_data = initial_data, 
      **kwargs
    )

  def __len__(self):
    return self.engine.size() 

  def __getitem__(self, key):
    return self.engine.get(key) 

  def get(self, key):
    return self.engine.get(key) 
    
  def __setitem__(self, key, item):
    return self.engine.put(key, item, replace = True, insert = True) 
    
  def put(self, key, item, replace = True, insert = True):
    return self.engine.put(key, item, replace = True, insert = True) 

  def __contains__(self, key):
    return self.engine.contains(key) 

  def contains(self, key):
    return self.engine.contains(key) 

  def keys(self):
    return self.engine.keys()

class Dict():

  def __new__(cls, *args, **kwargs):
    return Dict.create_or_bind(*args, **kwargs)
    
  # @staticmethod
  # def create(
  #   name, namespace = None, repository = None, encoders = [Pickle()], 
  #   versioned = True, metadata = None, data = None, **kwargs
  # ):
  #   return DictPrototype(
  #     name, CreationMode.Create, 
  #     namespace, repository, 
  #     encoders, versioned, 
  #     metadata, data, 
  #     **kwargs
  #   )

  @staticmethod
  def create_or_bind(
    name, namespace = None, repository = None,  
    pipeline = Pipeline(Pickle()), 
    initial_metadata = None, initial_data = None, **kwargs
  ):
    return Dict_(
      name, CreationMode.CreateOrBind, 
      namespace, repository, 
      pipeline, initial_metadata, initial_data, 
      **kwargs
    )

  # def __init__(self, name, namespace = None, repository = None, **kwargs):
  #   Collection.__init__(
  #     self, name, CreationMode.Bind, 
  #     namespace = namespace , repository = repository, 
  #     **kwargs
  #   )
