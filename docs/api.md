
Listing of the types and functions defined in the *parkit* package.

## Classes

Type | Description
---|---
Array | Append-only list. Implements the [Sequence](https://docs.python.org/3/library/collections.abc.html) interface plus a subset of the mutating operations on a list.
Dict | Dictionary that implements the [MutableMapping](https://docs.python.org/3/library/collections.abc.html) interface.
LifoQueue | Like Queue, but with LIFO ordering.
Namespace | Provides access to objects in a namespace, similar to a directory in a filesystem.
Object | Base class for all *parkit* classes. Can be sub-classed to create custom shared objects. Attributes defined on the *Object* class are persistent and shared across processes.
Periodic | A scheduler that invokes a task at regular intervals from a starting datetime.
Pool | Manager for pool of daemon processes that tasks execute on.
Queue | A queue implementation that generally follows the [Queue](https://docs.python.org/3/library/queue.html#queue.Queue) interface.
Scheduler | Base class for task schedulers. Custom schedulers sub-class *Scheduler*.

## Functions
Function | Parameters | Description
---|---|---
bind_symbol | Bind a module symbol to a task.
bind_task | Get a task object from the task's name.
create_task | Create a task from a function.
get_storage_path | Get current storage path for current thread.
namespaces | Returns an iterator over namespaces.
self | When invoked in a task, returns a reference to the *Task* object.
set_storage_path | Set current storage path for current thread.
wait_until | Block until lambda function returns True.

## Context Managers
Function | Parameters | Description
---|---|---
transaction | Open a write transaction in a namespace. Only one write transaction will be active in a given namespace.
snapshot | Open a snapshot of a namespace if not already in a transaction. There is (effectively) no limit on the number of concurrent readers. Readers are not blocked by writers.

## Exceptions
Type | Description
---|---
ObjectExistsError | Tried to create a new object when an object of the same path already exists.
ObjectNotFoundError | Tried to access the state of an object that was dropped.
StoragePathError | Raised if a storage path is not set or the path is invalid.
TransactionError | Transaction failed due to an unexpected error.
