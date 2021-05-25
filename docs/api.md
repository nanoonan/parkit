
Complete listing of the types and functions defined in the *parkit* package.

!!!note
    All operations on class instances by default execute in an implicit transaction. Use a context manager to group operations into the same explicit transaction.

## Classes

Type | Description
---|---
Dict | Dictionary that implements the [MutableMapping](https://docs.python.org/3/library/collections.abc.html) interface.
LifoQueue | Like Queue, but with LIFO ordering.
Log | Append-only list. Implements the [Sequence](https://docs.python.org/3/library/collections.abc.html) interface.
Object | Base class for all *parkit* classes.
Queue | A queue implementation that generally follows the [Queue](https://docs.python.org/3/library/queue.html#queue.Queue) interface.
Process | Unix-like daemon process that runs in a pool.

## Functions
Function | Parameters | Description
---|---|---
clean | status_filter | lslsls
get_namespace_size | namespace | Returns current maximum size of the storage backing a namespace.
get_pool_size | None | Returns current size of process pool.
killall | None | Shutdown the process pool.
namespaces | None | Iterator over created namespaces.
objects | namespace | Iterator over info about objects in a namespace. Returns tuples of (object path, object descriptor).
processes | status_filter | Iterator over processes. Optional status filter will return only processes that match the filter, or every process if the filter is None.
set_namespace_size | namespace, size | Change the maximum size in bytes of the storage backing a namespace. Default is 1GB.
set_pool_size | size | Change size of process pool dynamically.
set_process_priority | priority | Set priority of a running process. Only defined on Windows.

## Context Managers
Function | Parameters | Description
---|---|---
transaction | obj, zero_copy, isolated | Open a write transaction in a namespace. Only one write transaction will be active in a given namespace.
snapshot | obj, zero_copy | Open a read transaction in a namespace. There is (effectively) no limit on the number of concurrent read transactions. Readers are not blocked by writers.

## Exceptions
Type | Description
---|---
ContextError | Tried to modify objects from different namespaces in the same transactional context.
ObjectExistsError | Tried to create a new object when an object of the same path already exists.
ObjectNotFoundError | Tried to access the state of an object that was dropped.
TransactionError | Transaction failed due to an unexpected error.

## Enums
Type | Description
---|---
ProcessPriority | Defines process priority as High, Normal, or Low

## Type Annotations

All of the code is type annotated. Use the Python *help* command to return the parameters and parameter types for a function or class method.
```python
>>> help(objects)

Help on function objects in module parkit.storage.namespace:

objects(namespace: Union[str, NoneType] = None) -> Iterator[Tuple[str, parkit.typeddicts.Descriptor]]

```
```python
>>> help(Dict.__init__)

Help on function __init__ in module parkit.adapters.dict:

__init__(self, path: str, /, *, create: bool = True, bind: bool = True, versioned: bool = True)
    Initialize self.  See help(type(self)) for accurate signature.
```
