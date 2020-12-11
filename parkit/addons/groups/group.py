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

class GroupPrototype(Collection):  

  def __init__(self, name, namespace, install_path, encoders, versioned, **kwargs):      
    
    super().__init__(
      name, namespace = namespace, install_path = install_path, 
      serialization = Serialization(*encoders), versioned = versioned, 
      integer_keys = False, create = True, **kwargs
    )

    self._log = 
    
  def __len__(self):
    return self.engine.size()

  def size(self):
    return self.engine.size()

  def create(self, clock):
    if not issubclass(clock, Clock):
      raise InvalidArgument()

  def terminate(self, instance):
    if not issubclass(clock, ClockInstance):
      raise InvalidArgument()

  def instances(self):
    return self.engine.keys()

class Group(GroupPrototype):

  @staticmethod
  def create(
    name, namespace = None, install_path = None, encoders = [Pickle()], 
    versioned = True, metadata = None, **kwargs
  ):
    return GroupPrototype(
      name, namespace = namespace, install_path = install_path, 
      encoders = encoders, versioned = versioned, metadata = metadata,
      **kwargs
    )

  def __init__(self, name, namespace = None, install_path = None, **kwargs):
    Collection.__init__(
      self, name, namespace = namespace , install_path = install_path, 
      create = False, **kwargs
    )
