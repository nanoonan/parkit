
	#
  # API
  #

  def delete(self, key = None, first = True):
    if key is not None:
      key = self._pipeline.encode_key(key)
    try:
      result = False
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = False)
      if key is None:
        exists = thread.local.cursor.first() if first else thread.local.cursor.last()
        if not exists:
          return False
        else:
          result = thread.local.transaction.delete(thread.local.cursor.key(), db = thread.local.database)
      else:
        result = thread.local.transaction.delete(key, db = thread.local.database)
      return result
    except BaseException as e:
      thread.local.abort(e)
    finally:
      self._close_context(ctx, result)

  def clear(self):
    try:
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = False)
      thread.local.transaction.drop(thread.local.database, delete = False)
      return None
    except BaseException as e:
      thread.local.abort(e)
    finally:
      self._close_context(ctx, True)

  def drop(self):
    try:
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = False)
      thread.local.transaction.drop(thread.local.database, delete = True)
      thread.local.transaction.delete(self._encoded_name, db = self._lmdb.version_database)
      self._lmdb.delete_database(thread.local.transaction, self._encoded_uuid, encoded_db_name = self._encoded_name)
      return None
    except BaseException as e:
      thread.local.abort(e)
    finally:
      self._close_context(ctx, True)

  def contains(self, key):
    key = self._pipeline.encode_key(key)
    try:
      ctx = self._open_context(mode = TransactionMode.Reader, cursor = True)
      result = thread.local.cursor.set_key(key)
      return result
    except BaseException as e:
      thread.local.abort(e)
    finally:
      self._close_context(ctx, False)

  def append(self, value, key = None):
    value = self._pipeline.encode_value(value)
    if key is not None:
      key = self._pipeline.encode_key(key)
    try:
      result = False
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = True if key is None else False)
      if key is None:
        if not thread.local.cursor.last():
          key = struct.pack('@N', 0)
        else:
          key = struct.pack('@N', struct.unpack('@N', thread.local.cursor.key())[0] + 1)
      result = thread.local.transaction.put(
        key, value, 
        append = True, overwrite = False, db = thread.local.database
      )
      return result
    except BaseException as e:
      thread.local.abort(e)
    finally:
      self._close_context(ctx, result)

  def add(self, value):
    raise NotImplementedError()

  def remove(self, value):
    raise NotImplementedError()

  def put(self, key, value, replace = True, insert = True):
    if not insert and not replace:
      return False
    key = self._pipeline.encode_key(key)
    value = self._pipeline.encode_value(value)
    try:
      result = False 
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = True if not insert else False)
      if not insert:
        if not thread.local.cursor.set_key(key):
          return False
      result = thread.local.transaction.put(
        key, value, append = False, overwrite = replace, 
        db = thread.local.database
      )       
      return result
    except BaseException as e:
      thread.local.abort(e)
    finally:
      self._close_context(ctx, result)

  def get(self, key = None, first = True, default = None):
    if key is not None:
      key = self._pipeline.encode_key(key)
    try:
      ctx = self._open_context(mode = TransactionMode.Reader, cursor = True if key is None else False)
      if key is None:
        exists = thread.local.cursor.first() if first else thread.local.cursor.last()
        if not exists:
          return default
        else:
          result = thread.local.cursor.value()
      else:
        result = thread.local.transaction.get(key, db = thread.local.database)
      return self._pipeline.decode_value(result) if result is not None else default
    except BaseException as e:
      thread.local.abort(e)
    finally:
      self._close_context(ctx, False)

  def size(self):
    try:
      ctx = self._open_context(mode = TransactionMode.Reader, cursor = False)
      return thread.local.transaction.stat(self._database)['entries']
    except BaseException as e:
      thread.local.abort(e)
    finally:
      self._close_context(ctx, False)
  
  def keys(self):
    raise NotImplementedError()
    # return thread.local.get_cursor().iternext(keys = True, values = False)
  
  def pop(self, key = None, first = True, default = None):
    if key is not None:
      key = self._pipeline.encode_key(key)
    try:
      result = None
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = False)
      if key is None:
        exists = thread.local.cursor.first() if first else thread.local.cursor.last()
        if not exists:
          return default
        else:
          result = thread.local.transaction.pop(thread.local.cursor.key(), db = thread.local.database)
      else:
        result = thread.local.transaction.pop(key, db = thread.local.database)
      return self._pipeline.decode_value(result) if result is not None else default
    except BaseException as e:
      thread.local.abort(e)
    finally:
      self._close_context(ctx, result is not None)

  def put_many(self, items, replace = True, insert = True):
    if not insert and not replace:
      return False 
    items = [(self._pipeline.encode_key(key), self._pipeline.encode(value)) for key, value in items]
    try:
      added = 0
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = True if not insert else False)
      if not insert:
        items_removed = len(items)
        items = [(key, value) for key, value in items if thread.local.cursor.set_key(key)]
        items_removed -= len(items)
      if len(items) == 0:
        return (0, 0)
      else:
        (consumed, added) = thread.local.cursor.put_many(
          items, append = False, overwrite = replace, db = thread.local.database
        )
        return (consumed + items_removed, added)  
    except BaseException as e:
      thread.local.abort(e)
    finally:
      self._close_context(ctx, added > 0)

import cloudpickle
import logging
import lz4.frame
import orjson

from parkit.storage import LMDBBase
from parkit.utility import create_class

logger = logging.getLogger(__name__)

try:
  from cPickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
except ImportError:
  from pickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL

class Encoder():

  @staticmethod
  def encode(obj, **kwargs):
    return obj

  @staticmethod
  def decode(obj, **kwargs):
    return obj

class LZ4(Encoder):

  @staticmethod
  def encode(obj, **kwargs):
    return lz4.frame.compress(obj)
    
  @staticmethod
  def decode(obj, **kwargs):
    return lz4.frame.decompress(obj) if obj is not None else None

# class Fernet(Encoder):

#   @staticmethod
#   def encode(obj, **kwargs):
#     return dumps(obj)
    
#   @staticmethod
#   def decode(obj, **kwargs):
#     return loads(obj) if obj is not None else None

# pipeline = lambda a, b, c, d, e: pipeline(d[-1].encode(a, b, c, **e), b, c, d[:-1], e) if len(d) else (a, b, c, d, e)

class Pickle(Encoder):

  @staticmethod
  def encode(obj):
    return dumps(obj) 
    
  @staticmethod
  def decode(obj):
    return loads(obj) if obj is not None else None

class CloudPickle(Encoder):

  @staticmethod
  def encode(obj, **kwargs):
    return cloudpickle.dumps(obj)
    
  @staticmethod
  def decode(obj, **kwargs):
    return cloudpickle.loads(obj) if obj is not None else None

class Json(Encoder):

  @staticmethod
  def default(obj):
    if issubclass(type(obj), LMDBBase):
      return obj.__getstate__()
    raise TypeError

  @staticmethod
  def deserialize(attrs):
    cls = create_class(attrs['__qualified_class_name__'])
    instance = cls.__new__(cls)
    instance.__setstate__(attrs)
    return instance
  
  @staticmethod
  def encode(obj, **kwargs):
    return orjson.dumps(obj, default = Json.default)
    
  @staticmethod
  def decode(obj, **kwargs):
    if obj is None:
      return None
    obj = orjson.loads(obj if not isinstance(obj, memoryview) else bytes(obj))
    if isinstance(obj, dict) and '__qualified_class_name__' in obj:
      return Json.deserialize(obj)
    else:
      return obj
import logging
import mmh3

logger = logging.getLogger(__name__)

class Hasher():

  @staticmethod
  def hash(key, value, encoded_value):
    return key

class MMH3(Hasher):

  MMH3_SEED = 42

  @staticmethod
  def encode(key = None, value = None, encoded_value = None, **kwargs):
    if encoded_value is not None:
      return mmh3.hash128(encoded_value, MMH3Hasher.MMH3_SEED, True, signed = False)
    else:
      raise NotImplementedError()
      
# def encode(self, obj):
#   if not len(self._encoders):
#     return obj
#   elif len(self._encoders) == 1:
#     return self._encoders[0].encode(obj, **self._runtime_kwargs)
#   else:
#     return reduce(lambda x, y: y.encode(x, **self._runtime_kwargs), [obj, *self._encoders])

# return types.MethodType(dict_update, obj)

# def decode(self, obj):
#   if not len(self._encoders):
#     return obj
#   elif len(self._encoders) == 1:
#     return self._encoders[0].decode(obj, **self._runtime_kwargs)
#   else:
#     return reduce(lambda x, y: y.decode(x, **self._runtime_kwargs), [obj, *reversed(self._encoders)])

# def hash(self, obj, value, encoded_value):
#   if not len(self._hashers):
#     return obj
#   elif len(self._hashers) == 1:
#     return self._hashers[0].encode(obj, value, encoded_value, **self._runtime_kwargs)
#   else:
#     return pipeline(obj, value, encoded_value, self._hashers, self._runtime_kwargs)[0]