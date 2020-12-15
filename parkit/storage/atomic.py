import ctypes
import logging
import struct

import parkit.storage.context as context

from parkit.constants import *
from parkit.exceptions import *
from parkit.storage.lmdbenv import TransactionMode
from parkit.utility import *

from contextlib import contextmanager

logger = logging.getLogger(__name__)

@contextmanager
def atomic_write(*args, **kwargs):
  if not len(args):
    raise RuntimeError('argument missing')
  txn = None
  try:
    txn = args[0]._environment.begin(
      db = None, write = True, buffers = True,
      parent = None if not len(context.transaction_stack) else context.transaction_stack[-1]
    )
    context.transaction = txn
    context.transaction_stack.append(context.transaction)
    context.changed.append(set())
    yield txn
    changed = context.changed[-1] 
    if len(changed):
      [engine._update_version(txn) for engine in [ctypes.cast(ref, ctypes.py_object).value for ref in changed]]
    txn.commit()
  except (ObjectDropped, ObjectNotFound) as e:
    if txn: txn.abort()
    raise e
  except BaseException as e:
    if txn: txn.abort()
    log_and_raise(e, TransactionAborted)
  finally:
    if txn:
      context.changed.pop()
      context.transaction_stack.pop()
      context.transaction = None if not len(context.transaction_stack) else context.transaction_stack[-1]

@contextmanager
def atomic_read(*args, **kwargs):
  if not len(args):
    raise RuntimeError('argument missing')
  txn = None
  try:
    txn = args[0]._environment.begin(
      db = None, write = False, buffers = True,
      parent = None if not len(context.transaction_stack) else context.transaction_stack[-1]
    )
    context.transaction = txn
    context.transaction_stack.append(context.transaction)
    yield txn
    txn.commit()
  except (ObjectDropped, ObjectNotFound) as e:
    if txn: txn.abort()
    raise e
  except BaseException as e:
    if txn: txn.abort()
    log_and_raise(e, TransactionAborted)
  finally:
    if txn:
      context.transaction_stack.pop()
      context.transaction = None if not len(context.transaction_stack) else context.transaction_stack[-1]



