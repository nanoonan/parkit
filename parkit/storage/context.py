# pylint: disable = broad-except
#
# reviewed: 6/16/21
#
import contextlib
import logging

from typing import (
    Any, Iterator, Set, Tuple
)

import lmdb

import parkit.storage.threadlocal as thread

logger = logging.getLogger(__name__)

@contextlib.contextmanager
def transaction_context(
    env: lmdb.Environment,
    *,
    write: bool = False,
    iterator: bool = False
) -> Iterator[Tuple[lmdb.Transaction, thread.CursorDict, Set[Any]]]:
    assert write and not iterator or not write
    stack_changed = False
    stack = thread.local.context.stacks[env]
    if not stack or \
    stack[-1].iterator or \
    write and not stack[-1].write:
        thread.local.context.push(env, write, iterator)
        stack_changed = True
    try:
        error = None
        yield stack[-1].transaction, stack[-1].cursors, stack[-1].changed
    except GeneratorExit:
        pass
    except BaseException as exc:
        error = exc
    finally:
        if stack_changed:
            thread.local.context.pop(env, error)
        if error is not None:
            raise error
