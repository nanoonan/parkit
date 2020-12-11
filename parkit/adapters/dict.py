import logging

from parkit.adapters.collection import Collection
from parkit.constants import *
from parkit.decorators import (
  Pickle,
  Serialization
)
from parkit.exceptions import *
from parkit.utility import *

logger = logging.getLogger(__name__)

class DictPrototype(Collection):  

  def __init__(self, name, namespace, install_path, encoders, versioned, **kwargs):      
    
    super().__init__(
      name, namespace = namespace, install_path = install_path, 
      serialization = Serialization(*encoders), versioned = versioned, 
      integer_keys = False, create = True, **kwargs
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
    versioned = True, metadata = None, **kwargs
  ):
    return DictPrototype(
      name, namespace = namespace, install_path = install_path, 
      encoders = encoders, versioned = versioned, metadata = metadata,
      **kwargs
    )

  def __init__(self, name, namespace = None, install_path = None, **kwargs):
    Collection.__init__(
      self, name, namespace = namespace , install_path = install_path, 
      create = False, **kwargs
    )
