# Introduction

## Overview

The *parkit* package provides some basic Python classes that work within a multiprocessing environment. Dictionaries, queues, logs (append-only lists), custom objects, and processes are supported. All classes in *parkit* support concurrent access from multiple processes running in parallel. Data is stored in LMDB which offers good performance.

## Installation

```
python -m pip install .
```

## Features

1. All classes support persistent data and transactional access in multiprocessing environments.
1. Class instances are identified by a namespace and a name. Constructors take a path argument (namespace '/' name) to attach the local instance to the persistent state.
1. Explicit transactions can operate on multiple objects in the same namespace. Both read-write and read-only transactions are supported. Readers never block.
1. Collection classes (Dict, Queue, and Log) provided with standard Python interfaces.
1. Unix-like daemon processes via the Process class.
1. Can create custom classes with persistent attributes by sub-classing Object.
