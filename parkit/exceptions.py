import logging
import os

import parkit.storage.threadlocal as thread

logger = logging.getLogger(__name__)

class TransactionError(RuntimeError):
    pass

class ContextError(RuntimeError):
    pass

class ObjectExistsError(RuntimeError):
    pass

class ObjectNotFoundError(RuntimeError):
    pass

def log(exc_value: BaseException):
    if not thread.local.transaction:
        if not isinstance(exc_value, (SystemExit, KeyboardInterrupt, GeneratorExit)):
            logger.exception('Trapped error on pid %i', os.getpid())
