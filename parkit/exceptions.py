
class TransactionError(RuntimeError):
    pass

class ContextError(RuntimeError):
    pass

class ObjectExistsError(RuntimeError):
    pass

class ObjectNotFoundError(RuntimeError):
    pass

class StoragePathError(RuntimeError):
    pass
