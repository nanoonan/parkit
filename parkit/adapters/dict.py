import collections.abc
import logging

from parkit.storage import (
  Database,
  generic_dict_contains,
  generic_dict_get,
  generic_dict_delete,
  generic_dict_iter,
  generic_dict_put,
  generic_size,
  generic_clear,
  generic_dict_pop,
  generic_dict_popitem,
  generic_dict_setdefault,
  generic_dict_update,
  LMDBObject
)

try:
  from cPickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
except ImportError:
  from pickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL

logger = logging.getLogger(__name__)

class Dict(LMDBObject, collections.abc.MutableMapping):  

  def __init__(
    self, path, create = True, bind = True, versioned = False,
    on_create = lambda: None
  ):
    super().__init__(
      path, create = create, bind = bind, versioned = versioned, 
      on_create = on_create, databases = [{}, {}]
    )

  def __iter__(self):
    return self.keys()

  def __setitem__(self, key, value):
    return self.put(key, value)

  def __getitem__(self, key):
    return self.get(key)

  def __delitem__(self, key):
    return self.delete(key)

  def __len__(self):
    return self.size()

  def __contains__(self, key):
    return self.contains(key)

  def on_bind(self):
    self._get_attr = generic_dict_get(
      self, Database.First.value, self.attr_encode_key, self.attr_decode_value
    )
    self._put_attr = generic_dict_put(
      self, Database.First.value, self.attr_encode_key, self.attr_encode_value
    )
    self.delete = generic_dict_delete(self, Database.Second.value, self.dict_encode_key)
    self.size = generic_size(self, Database.Second.value)
    self.get = generic_dict_get(self, Database.Second.value, self.dict_encode_key, self.dict_decode_value)
    self.put = generic_dict_put(self, Database.Second.value, self.dict_encode_key, self.dict_encode_value)
    self.contains = generic_dict_contains(self, Database.Second.value, self.dict_encode_key)
    self.keys = generic_dict_iter(
      self, Database.Second.value, self.dict_decode_key, self.dict_decode_value, 
      keys = True, values = False
    )
    self.values = generic_dict_iter(
      self, Database.Second.value, self.dict_decode_key, self.dict_decode_value, 
      keys = False, values = True
    )
    self.items = generic_dict_iter(
      self, Database.Second.value, self.dict_decode_key, self.dict_decode_value, 
      keys = True, values = True
    )
    self.pop = generic_dict_pop(self, Database.Second.value, self.dict_encode_key, self.dict_decode_value)
    self.popitem = generic_dict_popitem(self, Database.Second.value, self.dict_decode_key, self.dict_decode_value)
    self.clear = generic_clear(self, Database.Second.value)
    self.update = generic_dict_update(self, Database.Second.value, self.dict_encode_key, self.dict_encode_value)
    self.setdefault = generic_dict_setdefault(
      self, Database.Second.value, self.dict_encode_key, self.dict_encode_value, self.dict_decode_value
    )

  def on_unbind(self):
    del to_wire['_get_attr']
    del to_wire['_put_attr']
    del to_wire['size']
    del to_wire['delete']
    del to_wire['get']
    del to_wire['put']
    del to_wire['contains']
    del to_wire['keys']
    del to_wire['values']
    del to_wire['items']
    del to_wire['clear']
    del to_wire['setdefault']
    del to_wire['update']
    del to_wire['popitem']
    del to_wire['pop']
    
  def attr_encode_key(self, key):
    return dumps(key)

  def attr_encode_value(self, value):
    return dumps(value)

  def attr_decode_value(self, value):
    return loads(value)

  def dict_encode_key(self, key):
    return dumps(key)

  def dict_decode_key(self, key):
    return loads(key)

  def dict_encode_value(self, value):
    return dumps(value)

  def dict_decode_value(self, value):
    return loads(value)


  
    

