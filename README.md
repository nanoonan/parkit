Status | Value
---|---
**Quality** | Development
**Tested** | Windows only
**Requires** | Python 3.8+

# Introduction
The *parkit* package provides some basic Python classes that work within a multiprocessing environment. Dictionaries, queues, logs (append-only lists), custom objects, and processes are supported. All classes in *parkit* support concurrent access from multiple processes running in parallel. Class instances are persistent and data is stored in LMDB which offers good performance.

The documentation is rather sparse at the moment since there is only one known user of this package, who happens to be the author.

Feel free to send questions, comments or feedback to: nanoonan at marvinsmind dot com.

# Installation
All LMDB databases created by *parkit* are stored in a path identified by the PARKIT_INSTALLATION_PATH environment variable. Make sure to set this environment variable to an appropriate value before installing/running *parkit*. The default value for PARKIT_INSTALLATION_PATH is the local temporary path.

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
parkit.set_namespace_size(5368709120, namespace = 'default')
```

# Transactions

All changes to the state of a *parkit* object execute within a transactional context. By default, each operation executes in an
implicit transactional context.

```
# An implicit transaction is started for this operation
obj['foo'] = 'new value'
```

You can also start an explicit transaction to modify multiple objects within a single transaction. A transaction may only modify objects within the same namespace. To specify the namespace to start a transaction on, pass any object from that namespace to the transaction context manager.

Grouping operations into a single transaction will provide a significant performance boost over performing each operation in a separate implicit transaction. 

```
obj1 = parkit.Dict('examples/mydict1')
obj2 = parkit.Dict('examples/mydict2')

# Update obj1 and obj2 in a single transaction
with parkit.transaction(obj1):
    obj1['foo'] = 'new value'
    obj2['bar'] = 'new value'
```

If you only want to read from multiple objects assuring a consistent state, you can open a snapshot transaction.

```
# Read from obj1 and obj1 within a read-only transactional context
with parkit.snapshot(obj1):
    val1 = obj1['foo']
    val2 = obj2['bar']
```

# Persistent Objects
You can create custom classes with named instances that automatically persist their attributes.
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
