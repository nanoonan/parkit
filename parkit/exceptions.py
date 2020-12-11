import logging

logger = logging.getLogger(__name__)

class ParKitException(Exception):
  pass

class SystemException(ParKitException):

  def __init__(self, e):
    self._exception = e

  @property
  def exception(self):
    return self._exception

class TransactionAborted(SystemException):
  pass

class InvalidPath(ParKitException):
  pass

class NotSerializableError(ParKitException):
  pass

class InvalidKey(ParKitException):
  pass

class InvalidOperation(ParKitException):
  pass

class ObjectExists(ParKitException):
  pass

class ObjectNotFound(ParKitException):
  pass
  
class ClassMismatch(ParKitException):
  pass

class InvalidArgument(ParKitException):
  pass

class InvalidId(ParKitException):
  pass

class InvalidContext(ParKitException):
  pass

class InvalidEnvironment(ParKitException):
  pass

class InvalidTransactionMode(ParKitException):
  pass

def log(e):
  if not issubclass(e, SystemException):
    logger.exception('trapped exception')
  
def log_and_raise(e, exc_type = None):
  if not issubclass(e, SystemException):
    logger.exception('trapped exception')
    if exc_type is None:
      raise SystemException(e)
    else:
      raise exc_type(e)
  else:
    if exc_type is None:
      raise e
    else:
      raise exc_type(e)

