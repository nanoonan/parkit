import logging

from parkit.exceptions import *
from parkit.storage.encoder import KeyEncoder
from parkit.utility import *
from functools import (
  lru_cache,
  reduce
)

logger = logging.getLogger(__name__)
    
def compose(*functions):
  return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)

class Pipeline():

  def __init__(self, *encoders, encode_keys = True, encode_values = True):
    self._encoders = list(encoders) if len(encoders) else None
    self._encode_keys = encode_keys
    self._encode_values = encode_values
    self._initialize()

  @property
  def has_key(self):
    return self._keygen is not None

  @property
  def key(self):
    return self._keygen.key

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

  @property
  def encoders(self):
    return self._encoders

  @encoders.setter
  def encoders(self, value):
    self._encoders = value

  def __getstate__(self):
    return {
      'encode_keys': self._encode_keys,
      'encode_values': self._encode_values,
      'encoders': [serialize_json_compatible(encoder) for encoder in self._encoders] if self._encoders is not None else None
    }

  def __setstate__(self, from_wire):
    self._encoders = [deserialize_json_compatible(encoder) for encoder in from_wire['encoders']] if from_wire['encoders'] is not None else None
    self._encode_keys = from_wire['encode_keys']
    self._encode_values = from_wire['encode_values']
    self._initialize()

  def set_runtime_kwargs(self, **kwargs):
    if self._encoders is not None:
      for encoder in self._encoders:
        encoder.set_runtime_kwargs(**kwargs)

  def _initialize(self):
    if self._encoders is not None:
      keygens = [encoder for encoder in self._encoders if issubclass(type(encoder), KeyEncoder)]
      self._keygen = keygens[-1] if len(keygens) else None
      if len(self._encoders) == 1:
        self._encode = self._encoders[0].encode
      else:
        self.encode = compose(*[encoder.encode for encoder in reversed(self._encoders)])
      deserializers = [encoder for encoder in self._encoders if not issubclass(type(encoder), KeyEncoder)]
      if len(deserializers) == 0:
        self._decode = None
      elif len(deserializers) == 1:
        self._decode = deserializers[0].decode
      else:
        self._decode = compose(*[encoder.decode for encoder in deserializers])
    else:
      self._encode = None
      self._decode = None
      self._hasher = None

  @lru_cache()
  def encode_key(self, obj):
    if self._encode is not None and self._encode_keys:
      return self._encode(obj)
    else:
      return obj

  def decode_key(self, obj):
    if obj is None:
      return None
    if self._decode is not None and self._encode_keys:
      return self._decode(obj)
    else:
      return obj

  def encode_value(self, obj):
    if self._encode is not None and self._encode_values:
      return self._encode(obj) 
    else:
      return obj 

  def decode_value(self, obj):
    if obj is None:
      return None
    if self._decode is not None and self._encode_values:
      return self._decode(obj)
    else:
      return obj



  