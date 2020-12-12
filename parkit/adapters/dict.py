import logging

from parkit.adapters.collection import (
  Collection,
  CreationMode
)
from parkit.constants import *
from parkit.decorators import (
  Pickle,
  Serialization
)
from parkit.exceptions import *
from parkit.utility import *

logger = logging.getLogger(__name__)

class DictPrototype(Collection):  

  def __init__(self, name, mode, namespace, install_path, encoders, versioned, metadata, data, **kwargs):      
    
    super().__init__(
      name, mode, namespace = namespace, install_path = install_path, 
      serialization = Serialization(*encoders), versioned = versioned, 
      integer_keys = False, metadata = metadata, data = data, 
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

class Dict(DictPrototype):

  @staticmethod
  def create(
    name, namespace = None, install_path = None, encoders = [Pickle()], 
    versioned = True, metadata = None, data = None, **kwargs
  ):
    return DictPrototype(
      name, CreationMode.Create, 
      namespace, install_path, 
      encoders, versioned, 
      metadata, data, 
      **kwargs
    )

  @staticmethod
  def create_or_bind(
    name, namespace = None, install_path = None, encoders = [Pickle()], 
    versioned = True, metadata = None, data = None, **kwargs
  ):
    return DictPrototype(
      name, CreationMode.CreateOrBind, 
      namespace, install_path, 
      encoders, versioned, 
      metadata, data, 
      **kwargs
    )

  def __init__(self, name, namespace = None, install_path = None, **kwargs):
    Collection.__init__(
      self, name, CreationMode.Bind, 
      namespace = namespace , install_path = install_path, 
      **kwargs
    )
