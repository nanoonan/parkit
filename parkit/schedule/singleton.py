import atexit
import collections
import copy
import logging
import threading

from parcolls.exceptions import *
from parcolls.utility import *

logger = logging.getLogger(__name__)

def process_singleton_atexit():
  for (instance, _, _) in ProcessSingleton.instances.values():
    try:
      instance.__atexit__()
    except Exception as e:
      log(e)

atexit.register(process_singleton_atexit)

class ProcessSingleton():

  instances = collections.defaultdict(lambda: None)
  
  def __new__(cls, *args, **kwargs):
    digest = create_string_digest(str(cls), create_object_digest(kwargs), *[create_object_digest(arg) for arg in args])
    if ProcessSingleton.instances[digest] is None:
      ProcessSingleton.instances[digest] = (object.__new__(cls), copy.deepcopy(args), copy.deepcopy(kwargs))
      ProcessSingleton.instances[digest][0].__initialized__ = False
      ProcessSingleton.instances[digest][0].__digest__ = digest
    return ProcessSingleton.instances[digest][0]
    
  def __init__(self, *args, **kwargs):
    if not self.__initialized__:
      self.__initialized__ = True
      self.__init_once__(*args, **kwargs)
      
  def __init_once__(self, *args, **kwargs):
    pass

  def __atexit__(self):
    pass 

  def __getnewargs_ex__(self):
    assert self.__initialized__
    (_, args, kwargs) = ProcessSingleton.instances[self.__digest__]
    return (args, kwargs)

  def __getstate__(self):
    return self.__digest__

  def __setstate__(self, state):
    assert state == self.__digest__
    if not self.__initialized__:
      (_, args, kwargs) = ProcessSingleton.instances[self.__digest__]
      self.__initialized__ = True
      self.__init_once__(*args, **kwargs)

  @property
  def uuid(self):
    return self.__digest__

class ThreadSingleton():

  thread_local = threading.local()
  
  def __new__(cls, *args, **kwargs):
    try:
      instances = ThreadSingleton.thread_local.instances
    except AttributeError:
      instances = ThreadSingleton.thread_local.instances = collections.defaultdict(lambda: None)
    digest = create_string_digest(str(cls), create_object_digest(kwargs), *[create_object_digest(arg) for arg in args])
    if instances[digest] is None:
      instances[digest] = (object.__new__(cls), copy.deepcopy(args), copy.deepcopy(kwargs))
      instances[digest][0].__initialized__ = False
      instances[digest][0].__digest__ = digest
    return instances[digest][0]
    
  def __init__(self, *args, **kwargs):
    if not self.__initialized__:
      self.__initialized__ = True
      self.__init_once__(*args, **kwargs)
        
  def __init_once__(self, *args, **kwargs):
    pass

  def __getnewargs_ex__(self):
    assert self.__initialized__
    (_, args, kwargs) = ThreadSingleton.thread_local.instances[self.__digest__]
    return (args, kwargs)

  def __getstate__(self):
    return self.__digest__

  def __setstate__(self, state):
    assert state == self.__digest__
    if not self.__initialized__:
      (_, args, kwargs) = ThreadSingleton.thread_local.instances[self.__digest__]
      self.__initialized__ = True
      self.__init_once__(*args, **kwargs)

  @property
  def uuid(self):
    return self.__digest__