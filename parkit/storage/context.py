# pylint: disable = broad-except
import contextlib
import logging

from typing import (
    Any, Iterator, Set, Tuple
)

import lmdb

import parkit.storage.threadlocal as thread

from parkit.storage.threadlocal import CursorDict

logger = logging.getLogger(__name__)

@contextlib.contextmanager
def transaction_context(
    env: lmdb.Environment,
    write: bool = False, inherit: bool = True, buffers: bool = True
) -> Iterator[Tuple[lmdb.Transaction, CursorDict, Set[Any]]]:
    thread.local.context.push(env, inherit, write, buffers)
    try:
        error = None
        txn, cursors, changed, _ = \
        thread.local.context.get(env, write = write)
        yield (txn, cursors, changed)
    except GeneratorExit:
        pass
    except BaseException as exc:
        error = exc
    finally:
        thread.local.context.pop(env, error)
