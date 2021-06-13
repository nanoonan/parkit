
class TransactionError(RuntimeError):
    pass

class ObjectNotFoundError(RuntimeError):
    pass

class StoragePathError(RuntimeError):
    pass

class SiteNotFoundError(RuntimeError):
    pass

class SiteNotSpecifiedError(RuntimeError):
    pass
