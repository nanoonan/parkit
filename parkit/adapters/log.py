import logging
import struct

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

class LogPrototype(Collection):  

  def __init__(self, name, mode, namespace, install_path, encoders, versioned, metadata, **kwargs):      
    
    super().__init__(
      name, mode, namespace = namespace, install_path = install_path, 
      serialization = None, versioned = versioned, 
      integer_keys = True, metadata = metadata, 
      **kwargs
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

  def open(self):
    self.db = self.engine.base_engine.environment.get_database(
        self.engine.base_engine.encoded_db_name, self.engine.base_engine.integer_keys
    )
    self.txn = self.engine.base_engine.environment.get_transaction()
    self.cursor = self.txn.cursor(db = self.db)

  def commit(self):
    self.txn.commit()

  def append(self, item):
    self.engine.append(item)
    # if not self.cursor.last():
    #     key = struct.pack('@N', 0)
    # else:
    #     key = struct.pack('@N', struct.unpack('@N', self.cursor.key())[0] + 1)
    # self.txn.put(
    #     key, item, append = True, overwrite = False, 
    #     db = self.db
    #   )

class Log(LogPrototype):

  @staticmethod
  def create_or_bind(
    name, namespace = None, install_path = None, encoders = [Pickle()], 
    versioned = False, metadata = None, **kwargs
  ):
    return LogPrototype(
      name, CreationMode.CreateOrBind, namespace = namespace, install_path = install_path, 
      encoders = encoders, versioned = versioned, metadata = metadata, 
      **kwargs
    )

  @staticmethod
  def create(
    name, namespace = None, install_path = None, encoders = [Pickle()], 
    versioned = False, metadata = None, **kwargs
  ):
    return LogPrototype(
      name, CreationMode.Bind, namespace = namespace, install_path = install_path, 
      encoders = encoders, versioned = versioned, metadata = metadata, 
      **kwargs
    )

  def __init__(self, name, namespace = None, install_path = None, **kwargs):
    Collection.__init__(
      self, name, CreationMode.Bind, namespace = namespace , install_path = install_path, 
      create = False, **kwargs
    )