import logging

logger = logging.getLogger(__name__)

class TransactionError(RuntimeError):
    pass

class ContextError(RuntimeError):
    pass

class ObjectExistsError(RuntimeError):
    pass

class ObjectNotFoundError(RuntimeError):
    pass
