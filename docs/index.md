The *parkit* package provides some basic Python classes that support multiprocessing programming. Dictionaries, queues, arrays (append-only lists), custom shared objects, and asynchronous tasks are supported. All classes in *parkit* support transactional, concurrent access from multiple processes. Data is stored in LMDB which offers good performance.

## Installation

```
python -m pip install parkit
```

## Motivation

The goal of *parkit* is to make basic multiprocessing programming as simple as possible without sacrificing performance. Collection classes allow sharing data between processes in a Pythonic manner. Collections are grouped into namespaces and are modified in transactions. One concurrent write transaction is allowed per namespace, but there is no effective limit on the number of concurrent read transactions. Readers are never blocked by writers.

Asynchronous tasks are easy to define and run on a managed pool of daemon processes (not child processes). Task scheduling is also supported.

## Credit

The *parkit* package is built on top of these excellent projects: [LMDB](http://www.lmdb.tech/doc/), [daemoniker](https://daemoniker.readthedocs.io/en/latest/), [dateparser](https://pypi.org/project/dateparser/), [watchdog](https://pypi.org/project/watchdog/), [cloudpickle](https://github.com/cloudpipe/cloudpickle) and [psutil](https://github.com/giampaolo/psutil). Any benefits of this package are largely due to those efforts, while the shortcomings are wholly those of *parkit*.
