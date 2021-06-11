
class ContextError(RuntimeError):
    pass

class TransactionError(RuntimeError):
    pass

class ObjectExistsError(RuntimeError):
    pass

class ObjectNotFoundError(RuntimeError):
    pass

class NamespaceNotFoundError(RuntimeError):
    pass

class StoragePathError(RuntimeError):
    pass

class SiteNotFoundError(RuntimeError):
    pass

class SiteNotSpecifiedError(RuntimeError):
    pass
