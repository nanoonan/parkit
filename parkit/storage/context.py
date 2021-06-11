# pylint: disable = broad-except, invalid-name
import contextlib
import enum
import logging

from typing import (
    Any, Iterator, Set, Tuple
)

import lmdb

import parkit.storage.threadlocal as thread

from parkit.exceptions import ContextError
from parkit.storage.threadlocal import CursorDict

logger = logging.getLogger(__name__)

class InheritMode(enum.Enum):
    Isolated = 0
    WriteExclusive = 1
    NotIsolated = 2

@contextlib.contextmanager
def transaction_context(
    env: lmdb.Environment,
    write: bool = False,
    inherit: InheritMode = InheritMode.NotIsolated,
    buffers: bool = True
) -> Iterator[Tuple[lmdb.Transaction, CursorDict, Set[Any]]]:
    stack_changed = True
    stack = thread.local.context.stacks[env]
    if not stack:
        thread.local.context.push(env, write, buffers)
    elif write and not stack[-1].write:
        if inherit.value != InheritMode.WriteExclusive.value:
            thread.local.context.push(env, write, buffers)
        else:
            raise ContextError()
    elif write and inherit.value == InheritMode.Isolated.value:
        thread.local.context.push(env, write, buffers)
    else:
        stack_changed = False
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
