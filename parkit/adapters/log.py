import logging
import struct

from parkit.adapters.collection import Collection
from parkit.constants import *
from parkit.decorators import (
  Pickle,
  Serialization
)
from parkit.exceptions import *
from parkit.utility import *

logger = logging.getLogger(__name__)

class LogPrototype(Collection):  

  def __init__(self, name, namespace, install_path, encoders, versioned, **kwargs):      
    
    super().__init__(
      name, namespace = namespace, install_path = install_path, 
      serialization = Serialization(*encoders, encode_keys = False), versioned = versioned, 
      integer_keys = True, create = True, **kwargs
    )

  def __len__(self):
    return self.engine.size()

  def size(self):
    return self.engine.size()

  def __getitem__(self, key):
    if not isinstance(key, int):
      raise InvalidKey()
    key = struct.pack('@N', key)
    return self.engine.get(key)

  def get(self, key):
    if not isinstance(key, int):
      raise InvalidKey()
    key = struct.pack('@N', key)
    return self.engine.get(key)

  def __contains__(self, key):
    if not isinstance(key, int):
      raise InvalidKey()
    key = struct.pack('@N', key)
    return self.engine.contains(key)

  def contains(self, key):
    if not isinstance(key, int):
      raise InvalidKey()
    key = struct.pack('@N', key)
    return self.engine.contains(key)

  def append(self, item):
    return self.engine.append(item)

class Log(LogPrototype):

  @staticmethod
  def create(
    name, namespace = None, install_path = None, encoders = [Pickle()], 
    versioned = True, metadata = None, **kwargs
  ):
    return LogPrototype(
      name, namespace = namespace, install_path = install_path, 
      encoders = encoders, versioned = versioned, metadata = metadata, 
      **kwargs
    )

  def __init__(self, name, namespace = None, install_path = None, **kwargs):
    Collection.__init__(
      self, name, namespace = namespace , install_path = install_path, 
      create = False, **kwargs
    )