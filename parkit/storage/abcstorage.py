#
  # API
  #

  def get_metadata(self, txn, uuid):
    obj = txn.get(key = uuid, db = self._metadata_database)
    return json.loads(obj.decode()) 
    
  def put_metadata(self, txn, uuid, obj):
    return txn.put(
      key = uuid, value = json.dumps(obj).encode('utf-8'), db = self._metadata_database, 
      overwrite = True
    )

  def get_version(self, txn, uuid):
    version = txn.get(key = uuid, db = self._version_database)
    return struct.unpack('@N', version)[0] if version is not None else 0

  def update_version(self, txn, uuid):
    version = txn.get(key = uuid, db = self._version_database)
    if version is None:
      version = 1
    else:
      version = struct.pack('@N', struct.unpack('@N', version)[0] + 1)
    return txn.put(
      key = uuid, value = version, db = self._version_database,
      overwrite = True
    )
    
  def get_metadata(self):
    if self._encoded_uuid is None:
      return None
    try:
      ctx = self._open_context(mode = TransactionMode.Reader, cursor = False)
      return self._environment.get_metadata(ctx.transaction, self._encoded_uuid)
    except BaseException as e:
      ctx.abort(e)
    finally:
      self._close_context(ctx, False)
  
  def put_metadata(self, item):
    if self._encoded_uuid is None:
      return None
    try:
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = False)
      return self._environment.put_metadata(ctx.transaction, self._encoded_uuid, item)
    except BaseException as e:
      ctx.abort(e)
    finally:
      self._close_context(ctx, False)

  def get_version(self):
    try:
      ctx = self._open_context(mode = TransactionMode.Reader, cursor = False)
      return self._environment.get_version(ctx.transaction, self._encoded_uuid)
    except BaseException as e:
      ctx.abort(e)
    finally:
      self._close_context(ctx, False)

  def delete(self, key = None, first = True):
    if key is not None:
      key = self._pipeline.encode_key(key)
    try:
      result = False
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = False)
      if key is None:
        exists = ctx.cursor.first() if first else ctx.cursor.last()
        if not exists:
          return False
        else:
          result = ctx.transaction.delete(ctx.cursor.key(), db = ctx.database)
      else:
        result = ctx.transaction.delete(key, db = ctx.database)
      return result
    except BaseException as e:
      ctx.abort(e)
    finally:
      self._close_context(ctx, result)

  def clear(self):
    try:
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = False)
      ctx.transaction.drop(ctx.database, delete = False)
      return None
    except BaseException as e:
      ctx.abort(e)
    finally:
      self._close_context(ctx, True)

  def drop(self):
    try:
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = False)
      ctx.transaction.drop(ctx.database, delete = True)
      ctx.transaction.delete(self._encoded_name, db = self._environment.version_database)
      self._environment.delete_database(ctx.transaction, self._encoded_uuid, encoded_db_name = self._encoded_name)
      return None
    except BaseException as e:
      ctx.abort(e)
    finally:
      self._close_context(ctx, True)

  def contains(self, key):
    key = self._pipeline.encode_key(key)
    try:
      ctx = self._open_context(mode = TransactionMode.Reader, cursor = True)
      result = ctx.cursor.set_key(key)
      return result
    except BaseException as e:
      ctx.abort(e)
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
        if not ctx.cursor.last():
          key = struct.pack('@N', 0)
        else:
          key = struct.pack('@N', struct.unpack('@N', ctx.cursor.key())[0] + 1)
      result = ctx.transaction.put(
        key, value, 
        append = True, overwrite = False, db = ctx.database
      )
      return result
    except BaseException as e:
      ctx.abort(e)
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
        if not ctx.cursor.set_key(key):
          return False
      result = ctx.transaction.put(
        key, value, append = False, overwrite = replace, 
        db = ctx.database
      )       
      return result
    except BaseException as e:
      ctx.abort(e)
    finally:
      self._close_context(ctx, result)

  def get(self, key = None, first = True, default = None):
    if key is not None:
      key = self._pipeline.encode_key(key)
    try:
      ctx = self._open_context(mode = TransactionMode.Reader, cursor = True if key is None else False)
      if key is None:
        exists = ctx.cursor.first() if first else ctx.cursor.last()
        if not exists:
          return default
        else:
          result = ctx.cursor.value()
      else:
        result = ctx.transaction.get(key, db = ctx.database)
      return self._pipeline.decode_value(result) if result is not None else default
    except BaseException as e:
      ctx.abort(e)
    finally:
      self._close_context(ctx, False)

  def size(self):
    try:
      ctx = self._open_context(mode = TransactionMode.Reader, cursor = False)
      return ctx.transaction.stat(self._database)['entries']
    except BaseException as e:
      ctx.abort(e)
    finally:
      self._close_context(ctx, False)
  
  def keys(self):
    raise NotImplementedError()
    # return ctx.get_cursor().iternext(keys = True, values = False)
  
  def pop(self, key = None, first = True, default = None):
    if key is not None:
      key = self._pipeline.encode_key(key)
    try:
      result = None
      ctx = self._open_context(mode = TransactionMode.Writer, cursor = False)
      if key is None:
        exists = ctx.cursor.first() if first else ctx.cursor.last()
        if not exists:
          return default
        else:
          result = ctx.transaction.pop(ctx.cursor.key(), db = ctx.database)
      else:
        result = ctx.transaction.pop(key, db = ctx.database)
      return self._pipeline.decode_value(result) if result is not None else default
    except BaseException as e:
      ctx.abort(e)
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
        items = [(key, value) for key, value in items if ctx.cursor.set_key(key)]
        items_removed -= len(items)
      if len(items) == 0:
        return (0, 0)
      else:
        (consumed, added) = ctx.cursor.put_many(
          items, append = False, overwrite = replace, db = ctx.database
        )
        return (consumed + items_removed, added)  
    except BaseException as e:
      ctx.abort(e)
    finally:
      self._close_context(ctx, added > 0)