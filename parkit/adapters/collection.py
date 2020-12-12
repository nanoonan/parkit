import enum
import logging

from parkit.adapters.transaction import EngineContext
from parkit.constants import *
from parkit.decorators import (
  Json,
  Pickle,
  Serialization
)
from parkit.exceptions import *
from parkit.storage import (
  get_storage_engine,
  TransactionMode
)
from parkit.utility import *

logger = logging.getLogger(__name__)

def normalize_install_path(install_path):
  if os.path.exists(install_path):
    if not os.path.isdir(install_path):
      raise InvalidPath()
  return os.path.abspath(install_path)

class CreationMode(enum.Enum):
  Create = 1
  Bind = 2
  CreateOrBind = 3

class Collection():

  def __init__(
    self, name, mode, namespace = None, install_path = None, serialization = None, 
    versioned = True, integer_keys = False, metadata = None, data = None, 
    **kwargs
  ):
    
    directory = metadata
    if install_path is None:
      install_path = getenv(INSTALL_PATH_ENVNAME)
    self._install_path = normalize_install_path(install_path)
    namespace = namespace if namespace is not None else DEFAULT_NAMESPACE
    self._name = create_id(name)
    self._namespace = create_id(namespace)
    
    self._metadata_engine = Serialization(Json()).decorate(
      get_storage_engine(
        self._install_path,
        context = self._namespace, db_name = METADATA_DATABASE_NAME, integer_keys = False,
        versioned = False
      )
    )

    created = False
    if mode.value == CreationMode.Create.value or (mode.value == CreationMode.CreateOrBind.value and not self._metadata_engine.contains(self._name)):
      metadata = dict() 
      metadata['class'] = get_qualified_class_name(self).replace('Prototype', '')
      #metadata['decorators'] = [serialize_json_compatible(decorator) for decorator in decorators]
      metadata['versioned'] = versioned
      metadata['integer_keys'] = integer_keys
      metadata['directory'] = directory
      if not self._metadata_engine.contains(self._name):
        with EngineContext(TransactionMode.Writer, self._metadata_engine) as metadata_engine:
          if not metadata_engine.contains(self._name):
            metadata_engine.put(self._name, metadata)
            created = True
          else:
            if mode.value == CreationMode.Create.value:
              raise ObjectExists()
      else:
        if mode.value == CreationMode.Create.value:
          raise ObjectExists()
    if mode.value == CreationMode.Bind.value or mode.value == CreationMode.CreateOrBind.value:     
      metadata = self._metadata_engine.get(self._name)
      if metadata is None:
        raise ObjectNotFound()
      if metadata['class'] != get_qualified_class_name(self).replace('Prototype', ''):
        raise ClassMismatch()
      
    self._data_engine = get_storage_engine(
      self._install_path,
      context = self._namespace, db_name = self._name, integer_keys = metadata['integer_keys'],
      versioned = metadata['versioned']
    )

    # for decorator in reversed([deserialize_json_compatible(decorator) for decorator in metadata['decorators']]):
    #   decorator.set_runtime_kwargs(**kwargs)
    #   self.push_decorator(decorator)
    
    if created and data is not None:
      for key, value in data.items():
        self._data_engine.put(key, value)

  @property
  def name(self):
    return self._name

  @property
  def namespace(self):
    return self._namespace

  @property
  def install_path(self):
    return self._install_path

  @property
  def version(self):
    return self._data_engine.version()

  @property
  def versioned(self):
    return self._metadata_engine.get(self._name)['versioned']
  
  @property
  def metadata(self):
    return self._metadata_engine.get(self._name)['directory']

  @property
  def engine(self):
    return self._data_engine

  @property
  def base_engine(self):
    return self._metadata_engine.base_engine

  @metadata.setter
  def metadata(self, directory):
    assert directory is None or isinstance(directory, dict)
    with EngineContext(TransactionMode.Writer, self._metadata_engine) as metadata_engine:
      metadata = metadata_engine.get(self._name)
      metadata['directory'] = directory
      metadata_engine.put(self._name, metadata, replace = True, insert = False)

  def pop_decorator(self):
    self._data_engine = self._data_engine.engine

  def push_decorator(self, decorator):
    self._data_engine = decorator.decorate(self._data_engine)
    
  def delete(self):
    with EngineContext(TransactionMode.Writer, self._metadata_engine, self._data_engine) as (metadata_engine, data_engine):
      data_engine.drop()
      metadata_engine.delete(self._name)
    