import logging

from parkit.constants import *
from parkit.exceptions import *
from parkit.storage import StorageEngineFactory
from parkit.utility import *

logger = logging.getLogger(__name__)

class Collection():

  def __init__(
    self, name, namespace = None, repository = None, 
    integer_keys = False, duplicates = False, versioned = False,
    pipeline = None, initial_metadata = None, initial_data = None, 
    create = True, validate_storage_properties = True, **kwargs
  ):
    
    if repository is None:
      repository = getenv(REPOSITORY_ENVNAME)
    namespace = namespace if namespace is not None else DEFAULT_NAMESPACE
    
    self._storage = self._engine = StorageEngineFactory(repository, name, namespace = namespace)

    created = self._engine.create_or_bind(
      integer_keys = integer_keys, duplicates = duplicates, versioned = versioned, create = create,
      validate_storage_properties = validate_storage_properties
    )

    if created:
      metadata = self._storage.get_metadata()
      metadata['class'] = get_qualified_class_name(self)
      metadata['pipeline'] = serialize_json_compatible(pipeline)
      metadata['directory'] = initial_metadata
      self._storage.put_metadata(metadata)
    else:
      metadata = self._storage.get_metadata()
      if metadata['class'] != get_qualified_class_name(self):
        raise ClassMismatch()
      pipeline = deserialize_json_compatible(metadata['pipeline'])
    
    pipeline.set_runtime_kwargs(**kwargs)
    self._engine.pipeline = pipeline

    if created and initial_data is not None:
      for key, value in initial_data.items():
        self._storage.put(key, value)

  @property
  def uuid(self):
    return self._engine.uuid

  @property
  def name(self):
    return self._engine.name 
    
  @property
  def namespace(self):
    return self._engine.namespace 

  @property
  def repository(self):
    return self._engine.repository 

  @property
  def version(self):
    return self._storage.get_version() 
            
  @property
  def metadata(self):
    return self._storage.get_metadata()['directory']
        
  @property
  def engine(self):
    return self._engine

  @property
  def storage(self):
    return self._storage 
    
  @metadata.setter
  def metadata(self, directory):
    assert directory is None or isinstance(directory, dict)
    metadata = self._storage.get_metadata()
    metadata['directory'] = directory
    self._storage.put_metadata(metadata)
    
  def pop_decorator(self):
    self._storage = self._storage.storage
    
  def push_decorator(self, decorator):
    self._storage = decorator.decorate(self._storage)
    
  def drop(self):
    self._storage.drop()
        