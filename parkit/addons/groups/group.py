import logging

from parkit.adapters import (
  Collection,
  CreationMode,
  Dict, 
  Log
)
from parkit.constants import *
from parkit.decorators import (
  AccessControl,
  AccessPermission,
  Pickle,
  Serialization
)
from parkit.exceptions import *
from parkit.utility import *
from parkit.addons.groups.clustertools import start_cluster
from parkit.addons.groups.constants import *

logger = logging.getLogger(__name__)

class GroupPrototype(Collection):  

  def __init__(self, name, mode, namespace, repository, encoders, versioned, metadata, data, **kwargs):      
    
    super().__init__(
      name, mode, namespace = namespace, repository = repository, 
      serialization = Serialization(*encoders), versioned = versioned, 
      integer_keys = False, metadata = metadata, data = data, 
      **kwargs
    )

    # try:
    #   self._queue = Queue.create(
    #     MONITOR_QUEUE_NAME, namespace = GROUPS_NAMESPACE
    #   ) 
    # except ObjectExistsError:
    #   self._queue = Queue(MONITOR_QUEUE_NAME, namespace = GROUPS_NAMESPACE)
    
  def __len__(self):
    return self.engine.size()

  def size(self):
    return self.engine.size()

  # def create(self, clock):
  #   if not issubclass(type(clock), Clock):
  #     raise InvalidArgument()

  # def terminate(self, instance):
  #   if not issubclass(type(clock), ClockInstance):
  #     raise InvalidArgument()

  def instances(self):
    return self.engine.keys()

class Group(GroupPrototype):

  intialized = False

  @staticmethod
  def get_settings():
    settings = Dict.create_or_bind(
      SETTINGS_NAME, namespace = GROUPS_NAMESPACE, 
      data = dict(
        process_termination_timeout = DEFAULT_PROCESS_TERMINATION_TIMEOUT,
        cluster_size = DEFAULT_CLUSTER_SIZE
      ) 
    )
    return AccessControl(
      settings,
      permissions = [
        AccessPermission.AllowRead,
        AccessPermission.AllowReplace
      ]
    )

  @staticmethod
  def init():
    start_cluster(Group.get_settings())
    Group.initialized = True

  @staticmethod
  def create_or_bind(
    name, namespace = None, repository = None, encoders = [Pickle()], 
    versioned = True, metadata = None, **kwargs
  ):
    if not Group.initialized:
      raise NotAvailable()
    return GroupPrototype(
      name, CreationMode.Create,
      namespace, repository, 
      encoders, versioned, 
      metadata, None,
      **kwargs
    )

  @staticmethod
  def create(
    name, namespace = None, repository = None, encoders = [Pickle()], 
    versioned = True, metadata = None, **kwargs
  ):
    if not Group.initialized:
      raise NotAvailable()
    return GroupPrototype(
      name, CreationMode.Create,
      namespace, repository, 
      encoders, versioned, 
      metadata, None,
      **kwargs
    )

  def __init__(self, name, namespace = None, repository = None, **kwargs):
    if not Group.initialized:
      raise NotAvailable()
    Collection.__init__(
      self, name, CreationMode.Bind,
      namespace = namespace, repository = repository, 
      **kwargs
    )
