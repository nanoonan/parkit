Status | Value
---|---
**Quality** | Pre-Alpha
**Tested** | Windows only
**Requires** | Python 3.8+

# Overview
The *parkit* package provides some basic Python classes that support multiprocessing programming. Dictionaries, queues, arrays (append-only lists), custom shared objects, file-like objects, and asynchronous tasks (where each task runs in a separate daemon process) are supported. All classes in *parkit* support transactional, concurrent access from multiple processes. Data is stored in LMDB which offers good performance.

Tasks are typically created using the *asyncable* decorator for functions. Tasks are runnable synchronously or asynchronously. An asynchronous task is scheduled to run on a pool of daemon worker processes. A cron-like scheduling facility is also available to schedule tasks to run one or more times in the future.  

Collections classes like dictionaries implement standard Python interfaces but operations always execute in a transactional context across processes. A transaction may be implicit or explicit. A transaction context manager is available to open an explicit transaction allowing users to modify multiple objects within the same transaction.

A useful feature of LMDB is that data is memory mapped from the database file. Zero-copy access is available to the memory mapped segments. One example of this is the *File* object that supports memory mapped Numpy arrays with zero copy (read) access. 

With LMDB, readers never block but only one writer is allowed per namespace. *parkit* supports multiple namespaces organized hierarchically similar to a file system directory tree. Thus, with namespace partitioning multiple concurrent readers and writers is possible.

Some code examples in the form of Jupyter notebooks are in **parkit/tests**.

The **docs** directory is currently out of date.

# Installation
```
python -m pip install .
```
