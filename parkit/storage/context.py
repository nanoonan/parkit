# pylint: disable = broad-except, protected-access, unsubscriptable-object
import contextlib
import logging

from typing import (
    Any, Iterator
)

import lmdb

import parkit.storage.threadlocal as thread

logger = logging.getLogger(__name__)

@contextlib.contextmanager
def transaction_context(
    env: lmdb.Environment,
    write: bool = False, inherit: bool = True, buffers: bool = True
) -> Iterator[Any]:
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
