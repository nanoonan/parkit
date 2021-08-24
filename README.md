Status | Value
---|---
**Quality** | Pre-Alpha
**Tested** | Windows only
**Requires** | Python 3.8+

# Overview
The *parkit* package provides some basic Python classes that support multiprocessing programming. Dictionaries, queues, arrays (append-only lists), custom shared objects, and asynchronous tasks (where each task runs in a separate process) are supported. All classes in *parkit* support transactional, concurrent access from multiple processes. Data is stored in LMDB which offers good performance.

Some code examples in the form of Jupyter notebooks are in **parkit/tests**.

The **docs** directory is currently out of date.

# Installation
```
python -m pip install .
```
