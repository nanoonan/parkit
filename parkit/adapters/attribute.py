import logging
import types

logger = logging.getLogger(__name__)

class Attr:

  def __init__(self, readonly = False, encode_value = None, decode_value = None):
    self._readonly = readonly
    self._encode_value = encode_value
    self._decode_value = decode_value
  
  def __set_name__(self, owner, name):
    self.public_name = name
    self.private_name = '_' + name

  def __get__(self, obj, objtype = None):
    if self._decode_value:
      if not hasattr(obj, '_'.join([self.private_name, 'decode_value'])):
        setattr(obj, '_'.join([self.private_name, 'decode_value']), types.MethodType(self._decode_value, obj))
      value = obj._get_attr(self.private_name, decode_value = getattr(obj, '_'.join([self.private_name, 'decode_value'])))
    else:
      value = obj._get_attr(self.private_name)
    return value

  def __set__(self, obj, value):
    if self._readonly:
      raise AttributeError()
    if self._encode_value:
      if not hasattr(obj, '_'.join([self.private_name, 'encode_value'])):
        setattr(obj, '_'.join([self.private_name, 'encode_value']), types.MethodType(self._encode_value, obj))
      obj._put_attr(self.private_name, value, encode_value = getattr(obj, '_'.join([self.private_name, 'encode_value'])))
    else:
      obj._put_attr(self.private_name, value)
    