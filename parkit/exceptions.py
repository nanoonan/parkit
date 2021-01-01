import logging
import os

from typing import Optional

logger = logging.getLogger(__name__)

class ParkitError(RuntimeError):
    pass

class TransactionError(ParkitError):
    pass

class ContextError(TransactionError):
    pass

class ObjectExistsError(TransactionError):
    pass

class ObjectNotFoundError(TransactionError):
    pass

class TimeoutError(RuntimeError):
    pass

def abort(exc_value: Optional[BaseException] = None) -> None:
    if exc_value:
        if not issubclass(type(exc_value), TransactionError):
            log_and_raise(exc_value, TransactionError)
        raise exc_value
    raise TransactionError()

def log(exc_value: BaseException) -> None:
    if not issubclass(type(exc_value), ParkitError):
        if not isinstance(exc_value, (SystemExit, KeyboardInterrupt, GeneratorExit)):
            logger.exception('Trapped error on pid %i', os.getpid())

def log_and_raise(exc_value: BaseException, exc_type: type = None) -> None:
    if not issubclass(type(exc_value), ParkitError):
        if isinstance(exc_value, (SystemExit, KeyboardInterrupt, GeneratorExit)):
            raise exc_value
        logger.exception('Trapped error on pid %i', os.getpid())
        if exc_type is None:
            raise ParkitError() from exc_value
        raise exc_type() from exc_value
    raise exc_value
