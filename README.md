# Introduction
The *parkit* package provides some basic Python-like objects that work within a multiprocessing environment. Dictionaries, queues, logs (append-only lists), custom objects, and processes are supported. All classes in *parkit* are thread and process safe. Data is stored in LMDB which offers good performance.

# Installation
All LMDB databases are stored in a path identified by the PARKIT_INSTALLATION_PATH environment variable. Make sure to set this variable to an appropriate value before installing/running *parkit*. The default value for PARKIT_INSTALLATION_PATH is your local temporary path.

To install the *parkit* package, open a command prompt, navigate to the top-level directory of the *parkit* git installation, and run the following command.
```
python -m pip install .
```

# Collection Classes
Here is an example of the Dict class.
```
>>> import parkit
>>> obj = parkit.Dict('examples/mydict')
>>> obj['foo'] = 'bar'
>>> obj['foo']
'bar'
>>> list(obj.keys())
['foo']
```
Note the path passed to the constructor. Since *parkit* classes are persistent, every instance is referenced by a path. Valid paths consist of a namespace and a name. In this case the namespace is 'examples' and the name is 'mydict'. Each namespace is a separate LMDB database. Since each LMDB database supports one concurrent writer and multiple concurrent readers, you can increase write concurrency by separating objects into different namespaces.

# Persistent Objects

# Processes (Experimental)




