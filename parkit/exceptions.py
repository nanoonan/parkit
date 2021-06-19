#
# reviewed: 6/14/21
#
class TransactionError(RuntimeError):
    pass

class ObjectNotFoundError(RuntimeError):
    pass

class ObjectExistsError(RuntimeError):
    pass

class StoragePathError(RuntimeError):
    pass

class SiteNotFoundError(RuntimeError):
    pass

class SiteNotSpecifiedError(RuntimeError):
    pass
