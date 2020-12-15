import importlib
import inspect
import json
import logging
import mmh3
import os
import re
import tempfile
import time

from parkit.constants import *
from parkit.exceptions import *

from functools import lru_cache

logger = logging.getLogger(__name__)

def deserialize_json_compatible(attrs):
  cls = create_class(attrs['__parkit_class__'])
  instance = cls.__new__(cls)
  instance.__setstate__(attrs)
  return instance

class Decoder(json.JSONDecoder):
  
  def __init__(self):
    json.JSONDecoder.__init__(self, object_hook = self.dict_to_object)
    
  def dict_to_object(self, attrs):
    if '_qualified_class_name' in attrs:
      cls = create_class(attrs['_qualified_class_name'])
      instance = cls.__new__(cls)
      instance.__setstate__(attrs)
      return instance
    else:
      return attrs

class Encoder(json.JSONEncoder):
  
  def default(self, obj):
    if hasattr(type(obj), '__json__'):
      return obj.__getstate__()
    else:
      return json.JSONEncoder.default(self, obj)
  
def json_dumps(obj):
  return json.dumps(obj, cls = Encoder)

def json_loads(obj):
  return json.loads(obj, cls = Decoder)

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
    raise MissingEnvironment()
  else:
    return value
  
def checkenv(name, type):
  try:
    if type == bool:
      if not getenv(name).upper() == 'FALSE' and not getenv(name).upper() == 'TRUE':
        raise MissingEnvironment()
    else:
      type(getenv(name))
  except Exception:
    raise MissingEnvironment()

def envexists(name):
  return os.getenv(str(name)) is not None
  
def setenv(name, value):
  os.environ[str(name)] = str(value)

def create_id(obj):
  if obj is None:
    raise InvalidIdentifier()
  id = str(obj)
  if id.startswith('__') and id.endswith('__'):
    raise InvalidIdentifier()
  if id.isascii() and id.replace('_', '').replace('-', '').isalnum():
    return id
  else:
    raise InvalidIdentifier()
  
def create_string_digest(*segments):
  return str(mmh3.hash128(''.join([str(segment) for segment in segments]), MMH3_SEED, True, signed = False))
    
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