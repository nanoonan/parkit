import logging
import orjson
import struct
import uuid

import parkit.storage.context as context

from parkit.constants import *
from parkit.exceptions import *
from parkit.storage.lmdbenv import (
  LMDBEnvironment,
  TransactionMode
)
from parkit.utility import *

logger = logging.getLogger(__name__)

class LMDBEngine(LMDBEnvironment):

  __json__ = True

  def __init__(self, repository, name, namespace = None):
    self._encoded_name = create_id(name).encode('utf-8')
    super().__init__(repository, namespace)
    self._uuid = None
    self._versioned = False
    self._instances = []
    
  @property
  def uuid(self):
    if self._uuid is not None:
      return str(uuid.UUID(bytes = self._uuid))
    else:
      return None

  @property
  def versioned(self):
    return self._versioned

  @property
  def name(self):
    return self._encoded_name.decode()

  def _begin(self, implicit_mode = TransactionMode.Reader):
    if context.transaction is None:
      print('implicit')
      return self._environment.begin(
        db = None, write = False if implicit_mode.value == TransactionMode.Reader.value else True, 
        parent = None, buffers = True
      )
    else:
      print('explicit')
      return context.transaction

  def _commit(self, txn, changed = False):
    if context.transaction is None:
      if changed and self._versioned:
        cursor = None
        try:
          self._update_version(txn)
        except ObjectDropped as e:
          txn.abort()
          raise e
        except BaseException as e:
          txn.abort()
          log_and_raise(e, Transaction_aborted)
      txn.commit()
    else:
      if changed and self._versioned:
        context.changed[-1].add(id(self))

  def _abort(self, txn, exc_value = None):
    txn.abort()
    if exc_value is not None:
      if isinstance(exc_value, ObjectDropped) or isinstance(exc_value, ObjectNotFound):
        raise exc_value
      if context.transaction is None and not self.exists:
        raise ObjectDropped()
      else:
        log_and_raise(exc_value, Transaction_aborted)
    else:
      raise Transaction_aborted()

  def __getstate__(self):
    to_wire = super().__getstate__()
    del to_wire['_instances']
    return to_wire

  def __setstate__(self, from_wire):
    super().__setstate__(from_wire)
    if from_wire['_uuid'] is not None:
      txn = None
      try:
        txn = self._begin(implicit_mode = TransactionMode.Reader)
        instances = self._get_instances(txn, from_wire['_encoded_name'], from_wire['_uuid'])
        self._commit(txn)
      except BaseException as e:
        if txn: self._abort(txn, e)
        else: log_and_raise(e, Transaction_aborted) 
    else:
      instances = []
    local = dict(
      _instances = instances
    )
    self.__dict__ = {**from_wire, **local}

  def _update_version(self, txn):
    try:
      cursor = txn.cursor(db = self._version_database)
      if cursor.set_key(self._uuid):
        version = cursor.value()
        if version is None:
          version = 1
        else:
          version = struct.pack('@N', struct.unpack('@N', version)[0] + 1)
        assert cursor.put(key = self._uuid, value = version)
      else:
        raise ObjectDropped()
    finally:
      if cursor: cursor.close()

  def _create_or_bind(
    self, request_instances = [], request_versioned = True, 
    create = True
  ):
    txn = None
    try:
      metadata = packed_metadata = None
      if create:
        metadata, packed_metadata = self._create_metadata(
          self._encoded_name, request_instances = request_instances, request_versioned = request_versioned
        )
      txn = self._begin(implicit_mode = TransactionMode.Writer if create else TransactionMode.Reader)
      created, metadata, self._uuid, self._instances = self._create_or_bind_database(
        txn, self._encoded_name, metadata = metadata, packed_metadata = packed_metadata, 
        create = create
      )
      self._commit(txn)
      self._versioned = metadata['versioned']
      return (created, metadata)
    except BaseException as e:
      if txn: self._abort(txn, e)
      else: log_and_raise(e)

  def test(self):
    try:
      txn = self._begin(implicit_mode = TransactionMode.Writer)
      result = txn.put(key = 'foo'.encode('utf-8'), value = 'bar'.encode('utf-8'), db = self._instances[0])
      self._commit(txn, changed = True)
      return result
    except BaseException as e:
      self._abort(txn, e)

  @property
  def exists(self):
    txn = None
    try:
      txn = self._begin(implicit_mode = TransactionMode.Reader)
      stored_uuid = txn.get(key = self._encoded_name, db = self._instance_database)
      result = stored_uuid == self._uuid
      self._commit(txn)
      return result
    except BaseException as e:
      if txn: self._abort(txn, e)
      else: log_and_raise(e, Transaction_aborted)

  @property
  def version(self):
    txn = cursor = None
    try:
      txn = self._begin(implicit_mode = TransactionMode.Reader)
      cursor = txn.cursor(db = self._version_database)
      if cursor.set_key(self._uuid):
        version = cursor.value()
        version = struct.unpack('@N', version)[0] if version is not None else 0
      else:
        raise ObjectDropped()
      self._commit(txn)
      return version
    except ObjectDropped as e:
      self._abort(txn, e) 
    except BaseException as e:
      if txn: self._abort(txn, e) 
      else: log_and_raise(e, Transaction_aborted)
    finally:
      if cursor: cursor.close()

  def drop(self):
    txn = None
    try:
      txn = self._begin(implicit_mode = TransactionMode.Writer)
      stored_uuid = txn.get(key = self._encoded_name, db = self._instance_database)
      if stored_uuid == self._uuid:
        for database in self._instances:
          txn.drop(database, delete = True)
        assert txn.delete(key = self._encoded_name, db = self._instance_database)
        assert txn.delete(key = self._uuid, db = self._version_database)
        assert txn.delete(key = self._uuid, db = self._metadata_database)
      self._commit(txn)
    except BaseException as e:
      if txn: self._abort(txn, e)
      else: log_and_raise(e, Transaction_aborted)

  def get_metadata(self):
    txn = cursor = None
    try:
      txn = self._begin(implicit_mode = TransactionMode.Reader)
      cursor = txn.cursor(db = self._metadata_database)
      if cursor.set_key(self._uuid):
        item = bytes(cursor.value())
      else:
        raise ObjectDropped()
      self._commit(txn)
      return orjson.loads(item) if item is not None else None
    except ObjectDropped as e:
      self._abort(txn, e)
    except BaseException as e:
      if txn: self._abort(txn, e)
      else: log_and_raise(e, Transaction_aborted) 
    finally:
      if cursor: cursor.close()

  def put_metadata(self, item):
    txn = cursor = None
    try:
      serialized = orjson.dumps(item)
      txn = self._begin(implicit_mode = TransactionMode.Writer)
      cursor = txn.cursor(db = self._metadata_database)
      if cursor.set_key(self._uuid):
        assert cursor.put(key = self._uuid, value = serialized)
      else:
        raise ObjectDropped()
      self._commit(txn)
    except ObjectDropped as e:
      self._abort(txn, e)
    except BaseException as e:
      if txn: self._abort(txn, e)
      else: log_and_raise(e, Transaction_aborted)
    finally:
      if cursor: cursor.close()