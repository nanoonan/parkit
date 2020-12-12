
from abc import ABCMeta

class LoggedAgeAccess:

  def __get__(self, obj, objtype=None):
      value = obj._age
      logging.info('Accessing %r giving %r', 'age', value)
      return value

  def __set__(self, obj, value):
      logging.info('Updating %r to %r', 'age', value)
      obj._age = value

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

class ObjectPrototype(Collection):  

  def __init__(self, name, mode, namespace, install_path, encoders, versioned, metadata, data, **kwargs):      
    
    super().__init__(
      name, mode, namespace = namespace, install_path = install_path, 
      serialization = Serialization(*encoders), versioned = versioned, 
      integer_keys = False, metadata = metadata, data = data, 
      **kwargs
    )

class Object(ObjectPrototype):

  @staticmethod
  def create(
    name, namespace = None, install_path = None, encoders = [Pickle()], 
    versioned = True, metadata = None, **kwargs
  ):
    return ObjectPrototype(
      name, CreationMode.Create, 
      namespace, install_path, 
      encoders, versioned, 
      metadata, None, 
      **kwargs
    )

  @staticmethod
  def create_or_bind(
    name, namespace = None, install_path = None, encoders = [Pickle()], 
    versioned = True, metadata = None, **kwargs
  ):
    return DictPrototype(
      name, CreationMode.CreateOrBind, 
      namespace, install_path, 
      encoders, versioned, 
      metadata, None, 
      **kwargs
    )

  def __init__(self, name, namespace = None, install_path = None, **kwargs):
    Collection.__init__(
      self, name, CreationMode.Bind, 
      namespace = namespace , install_path = install_path, 
      **kwargs
    )

class Test(Object):

	def __init__(self, name):
		super().__init__(name)