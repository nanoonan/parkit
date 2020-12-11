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
    
  def clear(self, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowDelete.value not in self._permissions:
      return None
    return self.engine.clear(txn_context = txn_context)

  def drop(self, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowDrop.value not in self._permissions:
      return None
    return self.engine.drop(txn_context = txn_context)

  def version(self, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowRead.value not in self._permissions:
      return None
    return self.engine.version(txn_context = txn_context)

  def size(self, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowRead.value not in self._permissions:
      return None
    return self.engine.size(txn_context = txn_context)

  def keys(self, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowRead.value not in self._permissions:
      return None
    return self.engine.keys(txn_context = txn_context)

  def get(self, key, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowRead.value not in self._permissions:
      return None
    return self.engine.get(key, txn_context = txn_context)

  def contains(self, key, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowRead.value not in self._permissions:
      return False
    return self.engine.contains(key, txn_context = txn_context)

  def delete(self, key, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowDelete.value not in self._permissions:
      return False
    return self.engine.delete(key, txn_context = txn_context)

  def pop(self, key, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowDelete.value not in self._permissions or AccessPermission.AllowRead.value not in self._permissions:
      return None
    return self.engine.pop(key, txn_context = txn_context)

  def put(self, key, value, append = False, replace = True, insert = True, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowInsert.value not in self._permissions and AccessPermission.AllowReplace.value not in self._permissions:
      return False
    insert = True if len(self._permissions) == 0 or AccessPermission.AllowInsert.value in self._permissions else False
    replace = True if len(self._permissions) == 0 or AccessPermission.AllowReplace.value in self._permissions else False
    return self.engine.put(
      key, value, append = append, replace = replace, insert = insert,
      txn_context = txn_context
    )
      
  def append(self, value, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowInsert.value not in self._permissions:
      return False
    return self.engine.append(
      value, txn_context = txn_context
    )

  def put_many(self, items, append = False, replace = True, insert = True, txn_context = None):
    if len(self._permissions) and AccessPermission.AllowInsert.value not in self._permissions and AccessPermission.AllowReplace.value not in self._permissions:
      return False
    insert = True if len(self._permissions) == 0 or AccessPermission.AllowInsert.value in self._permissions else False
    replace = True if len(self._permissions) == 0 or AccessPermission.AllowReplace.value in self._permissions else False 
    return self.engine.put_many(
      items, append = append, replace = replace, insert = insert,
      txn_context = txn_context
    )
