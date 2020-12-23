import contextlib
import lmdb
import logging
import os
import parkit.storage.threadlocal as thread

from parkit.exceptions import (
  abort,
  log_and_raise,
  TransactionError
)
from parkit.storage.lmdbbase import LMDBBase
from parkit.storage.lmdbenv import get_environment
from parkit.utility import resolve

logger = logging.getLogger(__name__)

@contextlib.contextmanager
def context(obj, write = False, inherit = False, zerocopy = False):
  lmdb = obj if isinstance(obj, lmdb.Environment) else obj._lmdb
  try:
    inherit = inherit if thread.local.transaction else False
    if thread.local.transaction is None or not inherit:
      thread.local.transaction = lmdb.begin(
        write = write, buffers = zerocopy, 
        parent = None if not len(thread.local.transaction_stack) else thread.local.transaction_stack[-1]
      )
      thread.local.transaction_stack.append(thread.local.transaction)
      thread.local.changed_stack.append(set())
      thread.local.changed = thread.local.changed_stack[-1]
      thread.local.cursors_stack.append(thread.CursorDict())
      thread.local.cursors = thread.local.cursors_stack[-1]
    thread.local.property_stack.append((inherit, write))
    yield True
    inherit, write = thread.local.property_stack[-1]
    if not inherit:
      if write:
        if len(thread.local.changed):
          [obj.increment_version() for obj in thread.local.changed]
      thread.local.transaction.commit()
  except BaseException as e:
    abort(e)
  finally:
    thread.local.property_stack.pop()
    if not inherit:
      [cursor.close() for cursor in thread.local.cursors.values()]
      thread.local.cursors_stack.pop()
      thread.local.cursors = None if not len(thread.local.cursors_stack) else thread.local.cursors_stack[-1]
      thread.local.changed_stack.pop()
      thread.local.changed = None if not len(thread.local.changed_stack) else thread.local.changed_stack[-1]
      thread.local.transaction_stack.pop()
      thread.local.transaction = None if not len(thread.local.transaction_stack) else thread.local.transaction_stack[-1]
      
def transaction(*args, zerocopy = False):
  if len(args) and issubclass(type(args[0]), LMDBBase):
    lmdb = args[0]._lmdb
  elif len(args) and isinstance(args[0], str):
    lmdb, _, _, _ = get_environment(resolve(args[0], path = False))
  elif not len(args):
    lmdb, _, _, _ = get_environment(None)
  else:
    raise TypeError('Cannot determine namespace from arguments')
  return context(lmdb, write = True, inherit = False, zerocopy = zerocopy)

def snapshot(*args, zerocopy = False):
  if len(args) and issubclass(type(args[0]), LMDBBase):
    lmdb = args[0]._lmdb
  elif len(args) and isinstance(args[0], str):
    lmdb, _, _, _ = get_environment(resolve(args[0], path = False))
  elif not len(args):
    lmdb, _, _, _ = get_environment(None)
  else:
    raise TypeError('Cannot determine namespace from arguments')
  return context(lmdb, write = False, inherit = False, zerocopy = zerocopy)


