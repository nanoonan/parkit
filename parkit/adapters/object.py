import logging

from parkit.storage import (
  Database,
  generic_dict_get,
  generic_dict_put,
  LMDBObject
)

from typing import (
  Any, ByteString
)

try:
  from cPickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
except ImportError:
  from pickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL

logger = logging.getLogger(__name__)

class Object(LMDBObject):  

  def on_bind(self) -> None:
    self._get_attr = generic_dict_get(self, Database.First.value, self.attr_encode_key, self.attr_decode_value)
    self._put_attr = generic_dict_put(self, Database.First.value, self.attr_encode_key, self.attr_encode_value)

  def on_unbind(self) -> None:
    del to_wire['_get_attr']
    del to_wire['_put_attr']

  def attr_encode_key(self, key: Any) -> ByteString:
    return dumps(key)

  def attr_encode_value(self, value: Any) -> ByteString:
    return dumps(value)

  def attr_decode_value(self, value:ByteString) -> Any:
    return loads(value)

  

  
    

