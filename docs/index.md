The *parkit* package provides some basic Python classes that support multiprocessing programming. Dictionaries, queues, logs (append-only lists), custom objects, and daemon processes are supported. All classes in *parkit* support transactional, concurrent access from multiple processes. Data is stored in LMDB which offers good performance.

## Installation

```
python -m pip install parkit
```

!!!important
    Data is stored in the location pointed to by the PARKIT_STORAGE_PATH environment variable. This variable should be set before importing *parkit*. If the environment variable is not set when a database is accessed, *parkit* will automatically create a database in the system's temporary directory.

## Motivation

The goal of *parkit* is to make basic multiprocessing programming as simple as possible without sacrificing performance. Collection classes allow sharing data between processes in a Pythonic manner. The *Process* class implements a Unix-like daemon process, so its easy to launch and manage long-running processes while avoiding the problems inherent in child processes.

Currently the documentation covers the basic features of the API. More advanced features like zero-copy semantics and custom
encoding/decoding methods is not covered.

## Credit

The *parkit* package is largely a convenience wrapper over these excellent projects: [LMDB](http://www.lmdb.tech/doc/), [daemoniker](https://daemoniker.readthedocs.io/en/latest/), and [psutil](https://github.com/giampaolo/psutil). Any benefits of this package are largely due to those efforts, while the shortcomings are wholly those of *parkit*.
