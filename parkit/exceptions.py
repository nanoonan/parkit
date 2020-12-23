import logging
import os

logger = logging.getLogger(__name__)

class SystemError(RuntimeError):

  def __init__(self, obj = None):
    if isinstance(obj, str):
      super().__init__(obj)
    elif issubclass(type(obj), BaseException):
      self._wrapped = obj

  @property
  def caught(self):
    return self._wrapped

class TransactionError(SystemError):  
  pass

class ObjectExistsError(TransactionError):
  pass

class ObjectNotFoundError(TransactionError):
  pass
  
def abort(exc_value):
  if exc_value:
    if not issubclass(type(exc_value), TransactionError):
      log_and_raise(exc_value, TransactionError)
    else:
      raise exc_value
  else:
    raise TransactionError()

def log(exc_value):
  if not issubclass(type(exc_value), SystemError):
    if not isinstance(exc_value, SystemExit) and not isinstance(exc_value, KeyboardInterrupt) and not isinstance(exc_value, GeneratorExit):
      logger.exception('Trapped error')
  
def log_and_raise(exc_value, exc_type = None):
  if not issubclass(type(exc_value), SystemError):
    if isinstance(exc_value, SystemExit) or isinstance(exc_value, KeyboardInterrupt) or isinstance(exc_value, GeneratorExit):
      raise exc_value
    logger.exception('Trapped error on pid {0}'.format(os.getpid()))
    if exc_type is None:
      raise SystemError(exc_value)
    else:
      raise exc_type(exc_value)
  else:
    raise exc_value
    
