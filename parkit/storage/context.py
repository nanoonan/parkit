# pylint: disable = broad-except, protected-access, unsubscriptable-object
import collections
import contextlib
import logging
import traceback

from typing import (
    Any, ContextManager, Iterator
)

import lmdb

import parkit.storage.threadlocal as thread

from parkit.exceptions import ContextError
from parkit.storage.entitymeta import EntityMeta
from parkit.storage.environment import (
    get_database_threadsafe,
    get_environment_threadsafe
)
from parkit.utility import resolve_namespace

logger = logging.getLogger(__name__)

class CursorDict(dict):

    def __init__(self, txn: lmdb.Transaction):
        super().__init__()
        self._txn = txn

    def __getitem__(self, key: int) -> lmdb.Cursor:
        if not dict.__contains__(self, key):
            database = get_database_threadsafe(key)
            cursor = self._txn.cursor(db = database)
            dict.__setitem__(self, key, cursor)
            return cursor
        return dict.__getitem__(self, key)

@contextlib.contextmanager
def context(
    env: lmdb.Environment,
    write: bool = False, inherit: bool = False, buffers: bool = False
) -> Iterator[lmdb.Transaction]:
    ctx_stack = thread.local.context
    cur_ctx = ctx_stack[-1] if ctx_stack else None
    inherit = inherit if cur_ctx else False
    if not inherit and write and cur_ctx and not cur_ctx[3]:
        raise ContextError('Cannot open transaction in snapshot context')
    try:
        if not inherit:
            txn = env.begin(
                write = write, buffers = buffers,
                parent = None if not cur_ctx else cur_ctx[0]
            )
            ctx_stack.append((
                txn,
                CursorDict(txn),
                set(),
                write
            ))
            thread.local.transaction = ctx_stack[-1][0]
            thread.local.cursors = ctx_stack[-1][1]
            thread.local.changed = ctx_stack[-1][2]
        thread.local.arguments.append((inherit, write))
        try:
            yield thread.local.transaction
        except GeneratorExit:
            pass
        finally:
            inherit, write = thread.local.arguments.pop()
        if not inherit:
            if write:
                for obj in thread.local.changed:
                    if isinstance(type(obj), EntityMeta):
                        obj.increment_version()
            thread.local.transaction.commit()
    except BaseException as exc:
        traceback.print_exc()
        if not inherit:
            thread.local.transaction.abort()
        raise exc
    finally:
        if not inherit:
            ctx_stack = thread.local.context
            for cursor in ctx_stack[-1][1].values():
                cursor.close()
            ctx_stack.pop()
            if ctx_stack:
                thread.local.transaction = ctx_stack[-1][0]
                thread.local.cursors = ctx_stack[-1][1]
                thread.local.changed = ctx_stack[-1][2]
            else:
                thread.local.transaction = None
                thread.local.cursors = collections.defaultdict(lambda: None)
                thread.local.changed = set()

def transaction(
    obj: Any,
    zerocopy: bool = True,
    isolated: bool = False
) -> ContextManager:
    if isinstance(obj, str):
        env, _, _, _, _ = get_environment_threadsafe(resolve_namespace(obj))
    elif isinstance(type(obj), EntityMeta):
        env = obj._Entity__env
    else:
        raise TypeError()
    return context(env, write = True, inherit = not isolated, buffers = zerocopy)

def snapshot(
    obj: Any,
    zerocopy: bool = True
) -> ContextManager:
    if isinstance(obj, str):
        env, _, _, _, _ = get_environment_threadsafe(resolve_namespace(obj))
    elif isinstance(type(obj), EntityMeta):
        env = obj._Entity__env
    else:
        raise TypeError()
    return context(env, write = False, inherit = True, buffers = zerocopy)
