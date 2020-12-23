import distutils.util
import importlib
import inspect
import logging
import hashlib
import os
import re
import tempfile
import time

from parkit.constants import *
from parkit.exceptions import *

from functools import lru_cache

logger = logging.getLogger(__name__)

@lru_cache(None)
def create_class(qualified_class_name):
  try:
    module_path, class_name = qualified_class_name.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)
  except (ImportError, AttributeError) as e:
    log_and_raise(e)

def get_qualified_class_name(obj):
  if type(obj) == type:
    return obj.__module__ + '.' + obj.__name__
  else:
    return obj.__class__.__module__ + '.' + obj.__class__.__name__

@lru_cache(None)
def getenv(name, type = None):
  name = str(name)
  value = os.getenv(name)
  if value is None:
    raise ValueError('Environment variable {0} not found in environment'.format(name))
  else:
    if type is None or type is str:
      return value
    else:
      if type == bool:
        return bool(distutils.util.strtobool(value))
      elif type == int:
        return type(value)
      else:
        raise TypeError()

def checkenv(name, type):
  try:
    if type == bool:
      if not getenv(name).upper() == 'FALSE' and not getenv(name).upper() == 'TRUE':
        raise TypeError('Environment variable {0} has wrong type'.format(name))
    else:
      type(getenv(name))
      return True
  except Exception:
    raise TypeError('Environment variable has {0} wrong type'.format(name))

def envexists(name):
  return os.getenv(str(name)) is not None
  
def setenv(name, value):
  os.environ[str(name)] = str(value)

@lru_cache(None)
def resolve(obj: str, path: bool = True):
  if path and not obj:
    raise ValueError('None is not a valid path')
  elif not obj:
    return obj
  obj = [segment for segment in obj.split('/') if len(segment)]
  if not path:
    if all([segment.isascii() and segment.replace('_', '').replace('-', '').isalnum() for segment in obj]):
      return '/'.join(obj) if len(obj) else None
    else:
      raise ValueError('Namespace does not follow naming rules')
  else:
    if len(obj) and all([segment.isascii() and segment.replace('_', '').replace('-', '').isalnum() for segment in obj]):
      return (obj[0], None) if len(obj) == 1 else (obj[-1], '/'.join(obj[0:-1]))
    else:
      raise ValueError('Path does not follow naming rules')
  
def create_string_digest(*segments):
  return hashlib.sha1(''.join([str(segment) for segment in segments]).encode('utf-8')).hexdigest()
  
def polling_loop(interval, max_iterations = None, initial_offset = None):
  iteration = 0
  try:
    while True:
      start_ns = time.time_ns()
      yield iteration
      iteration += 1
      if max_iterations is not None and iteration == max_iterations:
          return
      if iteration == 1 and initial_offset is not None:
        sleep_duration = interval - ((time.time_ns() - start_ns - initial_offset) / 1e9)
      else:
        sleep_duration = interval - ((time.time_ns() - start_ns) / 1e9)
      if sleep_duration > 0:
          time.sleep(sleep_duration)
  except GeneratorExit:
    pass
    
def get_calling_modules():
  modules = []
  frame = inspect.currentframe()
  while frame:
    module = inspect.getmodule(frame)
    if module is not None:
      name = module.__name__
      package = module.__name__.split('.')[0]
      if package != 'parkit':
        modules.append(name)
    frame = frame.f_back
  return modules

class HighResolutionTimer():

  def start(self):
    self.start_ns = time.time_ns()

  def stop(self):
    elapsed = time.time_ns() - self.start_ns
    print('elapsed: {0} ms'.format(elapsed / 1e6))
    
  def __enter__(self):
    self.start()
    return self

  def __exit__(self, exc_type, exc_value, exc_traceback):
    self.stop()
    if exc_value is not None:
      raise exc_value