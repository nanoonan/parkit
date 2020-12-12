import importlib
import inspect
import logging
import mmh3
import os
import re
import tempfile
import time

from parkit.constants import *
from parkit.exceptions import *

from functools import (
  lru_cache,
  reduce
)

logger = logging.getLogger(__name__)

def serialize_json_compatible(obj):
  return (get_qualified_class_name(obj), obj.__getstate__())

def deserialize_json_compatible(value):
  obj = create_class(value[0])()
  obj.__setstate__(value[1])
  return obj

def compose(*functions):
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)

def create_class(qualified_name):
  try:
    module_path, class_name = qualified_name.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)
  except (ImportError, AttributeError) as e:
    log_and_raise(e)

def get_qualified_class_name(obj):
  return obj.__class__.__module__ + '.' + obj.__class__.__name__

@lru_cache(None)
def getenv(name):
  value = os.getenv(str(name))
  if value is None:
    raise InvalidEnvironment()
  else:
    return value
  
def checkenv(name, type):
  try:
    if type == bool:
      if not getenv(name).upper() == 'FALSE' and not getenv(name).upper() == 'TRUE':
        raise InvalidEnvironment()
    else:
      type(getenv(name))
  except Exception:
    raise InvalidEnvironment()

def envexists(name):
  return os.getenv(str(name)) is not None
  
def setenv(name, value):
  os.environ[str(name)] = str(value)

def create_id(obj):
  if obj is None:
    raise InvalidId()
  id = str(obj)
  if id.isascii() and id.replace('_', '').replace('-', '').isalnum():
    return id
  else:
    raise InvalidId()
  
def create_string_digest(*segments):
  return str(mmh3.hash128(''.join([str(segment) for segment in segments]), MMH3_SEED, True, signed = False))
    
def testgen(x):
  txn = x.engine.environment._env.begin(
    db = None, write = True, 
    parent = None, buffers = True
  )
  x.engine.set_context(txn)
  yield x
  x.engine.clear_context()

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