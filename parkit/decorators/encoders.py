import json
import logging
import lzma

from parkit.exceptions import *
from parkit.utility import *

from functools import singledispatchmethod

logger = logging.getLogger(__name__)

try:
  from cPickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
except ImportError:
  from pickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL

class Encodeable():

  def __init__(self, item):
    self._item = item

  @property
  def item(self):
    return self._item

  @property
  def empty(self):
    return self._item is None

  def to_bytes(self, f):
    self._item = f(self._item)
    return self

  def from_bytes(self, f):
    if self._item is not None:
      self._item = f(self._item)
    return self

class Key(Encodeable):
  pass
  
class Value(Encodeable):
  pass

class Encoder():

  def __init__(self, encode_keys = True, encode_values = True):
    self._encode_keys = encode_keys
    self._encode_values = encode_values

  @property
  def encode_keys(self):
    return self._encode_keys

  @encode_keys.setter
  def encode_keys(self, value):
    self._encode_keys = value

  @property
  def encode_values(self):
    return self._encode_values

  @encode_values.setter
  def encode_values(self, value):
    self._encode_values = value

  def to_bytes(self, encodeable):
    if isinstance(encodeable, Key) and self._encode_keys:
      return encodeable.to_bytes(self.encode)
    elif isinstance(encodeable, Value) and self._encode_values:
      return encodeable.to_bytes(self.encode)
    else:
      return encodeable

  def from_bytes(self, encodeable):
    if encodeable.empty:
      return encodeable
    if isinstance(encodeable, Key) and self._encode_keys:
      return encodeable.from_bytes(self.decode)
    elif isinstance(encodeable, Value) and self._encode_values:
      return encodeable.from_bytes(self.decode)
    else:
      return encodeable

  def encode(self, value):
    return value

  def decode(self, value):
    return value

  def set_runtime_kwargs(self, **kwargs):
    pass

  def __getstate__(self):
    return {
      'encode_keys': self._encode_keys,
      'encode_values': self._encode_values,
    }

  def __setstate__(self, from_wire):
    self._encode_keys = from_wire['encode_keys']
    self._encode_values = from_wire['encode_values']

class CompositionEncoder(Encoder):

  def __init__(self, encoders):
    self._to_bytes = compose(*[encoder.to_bytes for encoder in encoders])
    self._from_bytes = compose(*[encoder.from_bytes for encoder in encoders])

  def to_bytes(self, encodeable):
    return self._to_bytes(encodeable)
    
  def from_bytes(self, encodeable):
    return self._from_bytes(encodeable)

class LZMA(Encoder):

  def encode(self, value):
    return lzma.compress(value)
    
  def decode(self, decode):
    return lzma.decompress(value)

class Pickle(Encoder):

  def encode(self, value):
    return dumps(value)
    
  def decode(self, value):
    return loads(value)

class Json(Encoder):

  def encode(self, value):
    return json.dumps(value).encode('utf-8')

  def decode(self, value):
    return json.loads(bytes(value).decode())
