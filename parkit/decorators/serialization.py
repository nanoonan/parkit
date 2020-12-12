import logging

from parkit.decorators.encoders import (
  CompositionEncoder,
  Encoder,
  Key,
  Value
)
from parkit.decorators.mixins import *
from parkit.exceptions import *
from parkit.utility import *

logger = logging.getLogger(__name__)
    
class Serialization():

  def __init__(self, *encoders, encode_keys = True, encode_values = True):
    self._encoders = list(encoders)
    self._encode_keys = encode_keys
    self._encode_values = encode_values
    self._initialize()

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
      'encoders': [serialize_json_compatible(encoder) for encoder in self._encoders]
    }

  def __setstate__(self, from_wire):
    self._encoders = [deserialize_json_compatible(encoder) for encoder in from_wire['encoders']]
    self._encode_keys = from_wire['encode_keys']
    self._encode_values = from_wire['encode_values']
    self._initialize()

  def set_runtime_kwargs(self, **kwargs):
    for encoder in self._encoders:
      encoder.set_runtime_kwargs(**kwargs)
    
  def decorate(self, engine):
    return EncoderDecorator(engine, self._encoder, self._decoder, self._encode_keys, self._encode_values)

  def _initialize(self):
    if len(self._encoders) == 1:
      self._encoder = self._encoders[0]
      self._decoder = self._encoders[0]
    elif len(self._encoders):
      self._encoder = CompositionEncoder(reversed(self._encoders))
      self._decoder = CompositionEncoder(self._encoders)
    else:
      self._encoder = Encoder()
      self._decoder = Encoder()

class EncoderDecorator(
  Decorator, DefaultClear, DefaultSize, DefaultVersion, DefaultDrop, DefaultKeys
):

  def __init__(self, engine, encoder, decoder, encode_keys, encode_values):  
    super().__init__(engine)  
    self._encoder = encoder
    self._decoder = decoder
    self._encode_keys = encode_keys
    self._encode_values = encode_values

  def key(self, key):
    return self._encoder.encode(key) if self._encode_keys else key

  def value(self, value):
    return self._encoder.encode(value) if self._encode_values else value

  def delete(self, key, txn_context = None):
    try:
      key = self._encoder.to_bytes(Key(key)).item if self._encode_keys else key
      return self.engine.delete(key, txn_context = txn_context)
    except Exception as e:
      log_and_raise(e)
  
  def contains(self, key, txn_context = None):
    try:
      key = self._encoder.to_bytes(Key(key)).item if self._encode_keys else key
      return self.engine.contains(key, txn_context = txn_context)
    except Exception as e:
      log_and_raise(e)

  def get(self, key, txn_context = None):
    try:
      key = self._encoder.to_bytes(Key(key)).item if self._encode_keys else key
      value = self.engine.get(key, txn_context = txn_context)
      return self._decoder.from_bytes(Value(value)).item if self._encode_values else value
    except Exception as e:
      log_and_raise(e)

  def append(self, value, txn_context = None):
    try:
      value = self._encoder.to_bytes(Value(value)).item if self._encode_values else value
      return self.engine.append(
        value, 
        txn_context = txn_context
      )   
    except Exception as e:
      log_and_raise(e)

  def put(self, key, value, append = False, replace = True, insert = True, txn_context = None):
    try:
      key = self._encoder.to_bytes(Key(key)).item if self._encode_keys else key
      value = self._encoder.to_bytes(Value(value)).item if self._encode_values else value
      return self.engine.put(
        key, value, 
        append = append, replace = replace, insert = insert,
        txn_context = txn_context
      )   
    except Exception as e:
      log_and_raise(e)

  def pop(self, key, txn_context = None):
    try:
      key = self._encoder.to_bytes(Key(key)).item if self._encode_keys else key
      value = self.engine.pop(key, txn_context = txn_context)
      return self._decoder.from_bytes(Value(value)).item if self._encode_values else value
    except Exception as e:
      log_and_raise(e)

  def put_many(self, items, append = False, replace = True, insert = True, txn_context = None):
    try:
      items = [
        (
          self._encoder.to_bytes(Key(key)).item if self._encode_keys else key, 
          self._encoder.to_bytes(Value(value)).item if self._encode_values else value
        ) for key, value in items
      ]
      return self.engine.put_many(
        items, 
        append = append, replace = replace, insert = insert, 
        txn_context = txn_context
      )
    except Exception as e:
      log_and_raise(e)

  