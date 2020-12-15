import enum
import logging

from parkit.exceptions import *
from parkit.decorators.mixins import *
from parkit.utility import *

logger = logging.getLogger(__name__)

class AccessPermission(enum.Enum):
  AllowRead = 1
  AllowInsert = 2
  AllowReplace = 3
  AllowDelete = 4
  AllowDrop = 5
  AllowMetadataRead = 6
  AllowMetadataReplace = 7

class AccessControl():

  def __new__(cls, *args, **kwargs):
    collection = list(args)[0] 
    policy = kwargs['policy'] if 'policy' in kwargs else []
    assert all([isinstance(permission, AccessPermission) for permission in policy])
    collection.push_decorator(AccessPolicy(policy = policy))
    return collection

class AccessPolicy(DefaultSetRuntimeArgs):

  def __init__(self, policy = []):
    self._policy = policy

  def decorate(self, engine):
    return AccessPolicyDecorator(engine, policy = self._policy)

class AccessPolicyDecorator(Decorator):

  def __init__(self, engine, policy = []):
    super().__init__(engine) 
    self._permissions = [permission.value for permission in policy]

  @property
  def permissions(self):
    return self._permissions
    
  def clear(self):
    if len(self._permissions) and AccessPermission.AllowDelete.value not in self._permissions:
      return None
    return self.engine.clear()

  def drop(self):
    if len(self._permissions) and AccessPermission.AllowDrop.value not in self._permissions:
      return None
    return self.engine.drop()

  def get_version(self):
    if len(self._permissions) and AccessPermission.AllowMetadataRead.value not in self._permissions:
      return None
    return self.engine.get_version()

  def get_metadata(self):
    if len(self._permissions) and AccessPermission.AllowMetadataRead.value not in self._permissions:
      return None
    return self.engine.get_metadata()

  def put_metadata(self, item):
    if len(self._permissions) and AccessPermission.AllowMetadataReplace.value not in self._permissions:
      return None
    return self.engine.put_metadata(item)

  def size(self):
    if len(self._permissions) and AccessPermission.AllowRead.value not in self._permissions:
      return None
    return self.engine.size()

  def keys(self):
    if len(self._permissions) and AccessPermission.AllowRead.value not in self._permissions:
      return None
    return self.engine.keys()

  def get(self, key = None, first = True, default = None):
    if len(self._permissions) and AccessPermission.AllowRead.value not in self._permissions:
      return None
    return self.engine.get(key = key, first = first, default = default)

  def contains(self, key):
    if len(self._permissions) and AccessPermission.AllowRead.value not in self._permissions:
      return False
    return self.engine.contains(key)

  def delete(self, key = None, first = True):
    if len(self._permissions) and AccessPermission.AllowDelete.value not in self._permissions:
      return False
    return self.engine.delete(key = key, first = first)

  def pop(self, key = None, first = True, default = None):
    if len(self._permissions) and AccessPermission.AllowDelete.value not in self._permissions or AccessPermission.AllowRead.value not in self._permissions:
      return None
    return self.engine.pop(key = key, first = first, default = default)

  def put(self, key, value, replace = True, insert = True):
    if len(self._permissions) and AccessPermission.AllowInsert.value not in self._permissions and AccessPermission.AllowReplace.value not in self._permissions:
      return False
    insert = True if len(self._permissions) == 0 or AccessPermission.AllowInsert.value in self._permissions else False
    replace = True if len(self._permissions) == 0 or AccessPermission.AllowReplace.value in self._permissions else False
    return self.engine.put(
      key, value, replace = replace, insert = insert
    )
      
  def append(self, value, key = None):
    if len(self._permissions) and AccessPermission.AllowInsert.value not in self._permissions:
      return False
    return self.engine.append(
      value, key = key
    )

  def put_many(self, items, replace = True, insert = True):
    if len(self._permissions) and AccessPermission.AllowInsert.value not in self._permissions and AccessPermission.AllowReplace.value not in self._permissions:
      return False
    insert = True if len(self._permissions) == 0 or AccessPermission.AllowInsert.value in self._permissions else False
    replace = True if len(self._permissions) == 0 or AccessPermission.AllowReplace.value in self._permissions else False 
    return self.engine.put_many(
      items, replace = replace, insert = insert
    )
