import logging
import struct

from parkit.constants import *
from parkit.exceptions import *
from parkit.storage import TransactionMode
from parkit.utility import *

from contextlib import contextmanager
from itertools import groupby

logger = logging.getLogger(__name__)

def open_transaction(mode, *args):
  group = groupby([arg.base_engine.environment_id for arg in args])
  if not next(group, True) and not next(group, False):
    raise ContextMismatch()
  txn_engine = max(
    [(arg.base_engine.context_stack_depth(), arg.base_engine) for arg in args],
    key = lambda x: x[0]
  )[1]
  ctx_stack = txn_engine.get_context_stack()
  txn = txn_engine.open_transaction(
    mode = mode, 
    parent = None if len(ctx_stack) == 0 else ctx_stack[-1].transaction
  )
  return txn

def pre_execution(txn, mode, *args):
  [arg.base_engine.push_context(txn, mode = mode) for arg in args]

def post_execution(txn, aborted, *args):
  for arg in args:
    ctx = arg.base_engine.pop_context()
    if not aborted:
      if len(arg.base_engine.get_context_stack()) == 0:
        try:
          if arg.base_engine.changed and arg.base_engine.versioned:
            db = arg.base_engine.version_database
            result = txn.get(arg.base_engine.encoded_name, db = db)
            version = 0 if result is None else struct.unpack('@N', result)[0]
            txn.put(arg.base_engine.encoded_name, struct.pack('@N', version + 1), db = db)
        except BaseException as e:
          txn.abort()
          aborted = True
          log_and_raise(e, TransactionAborted)
  if not aborted:
    txn.commit()
  
@contextmanager
def atomic_write(*args, **kwargs):
  if not len(args):
    return
  txn = open_transaction(*args)
  pre_execution(txn, TransactionMode.Writer, *args)
  aborted = False
  try:
    yield None
  except BaseException as e:
    txn.abort()
    aborted = True
    log_and_raise(e, TransactionAborted)
  finally:
    post_execution(txn, aborted, *args)

@contextmanager
def atomic_read(*args, **kwargs):
  if not len(args):
    return
  txn = open_transaction(*args)
  pre_execution(txn, TransactionMode.Reader, *args)
  aborted = False
  try:
    yield None
  except BaseException as e:
    txn.abort()
    aborted = True
    log_and_raise(e, TransactionAborted)
  finally:
    post_execution(txn, aborted, *args)


