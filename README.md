# Introduction
The *parkit* package provides some basic Python-like objects that work within a multiprocessing environment. Dictionaries, queues, logs (append-only lists), custom objects, and processes are supported. All classes in *parkit* support concurrent access from multiple processes running in parallel. Data is stored in LMDB which offers good performance.

The documentation is rather sparse at the moment since there is only one known user of this package, who happens to be the author.

Questions or comments should be sent to: nanoonan at marvinsmind dot com.

# Installation
All LMDB databases are stored in a path identified by the PARKIT_INSTALLATION_PATH environment variable. Make sure to set this environment variable to an appropriate value before installing/running *parkit*. The default value for PARKIT_INSTALLATION_PATH is the local temporary path.

To install the *parkit* package, open a command prompt, navigate to the top-level directory of the *parkit* git installation, and run the following command.
```
python -m pip install .
```

# Collection Classes
Here is an example of the Dict class. The standard Python dict interface is supported.
```
>>> import parkit
>>> obj = parkit.Dict('examples/mydict')
>>> obj['foo'] = 'bar'
>>> obj['foo']
'bar'
>>> list(obj.keys())
['foo']
```
Note the path passed to the constructor. Since *parkit* classes are persistent, every instance is referenced by a path. Valid paths consist of a namespace and a name. In this case the namespace is 'examples' and the name is 'mydict'. Each namespace is a separate LMDB database. Since each LMDB database supports one concurrent writer and multiple concurrent readers, you can increase write concurrency by separating objects into different namespaces. Objects without path separators are stored in the 'default' namespace database.

By default, each LMDB database is 1GB. You can change the amount of space reserved for a database using the following command.
```
set_namespace_size(5368709120, namespace = 'default')
```

# Persistent Objects
You can create classes with named instances that automatically persist their attributes.
```
class MyClass(Object):

    local = 1

    def do_something(self):
    ...

x = MyClass('foo')
x.persist = 'bar'
```
In this example, the lifetime of the 'local' attribute is the same as the object instance. The rule is that any attribute declared in the class definition is not persistent. The 'persist' attribute is stored in the database. As in the collection classes, persistent attributes are process-safe.

Persistent attributes can also hold references to other persistent classes.
```
x.my_dict = parkit.Dict('examples/my_shared_dict')
```
