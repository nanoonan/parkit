import json
import logging
import lzma

from parkit.exceptions import *
from parkit.storage.encoder import Encoder 
from parkit.utility import *

logger = logging.getLogger(__name__)

try:
  from cPickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
except ImportError:
  from pickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL

import mmh3

from parkit.storage.encoder import KeyEncoder

class MMH3(KeyEncoder):

  def encode(self, obj):
    self.is_ordered = False
    self.key = mmh3.hash128(obj, MMH3_SEED, True, signed = False)
    return obj

# LZ4
# OrJson
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
