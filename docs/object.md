# Object
Every class in *parkit* derives from Object. The Object class represents an object with persistent attributes. It's easy to sub-class Object to create custom persistent objects. An instance of Object is accessible from multiple processes. Access to the persistent attributes occurs in either an implicit or explicit transaction.


```python
from parkit import Object

# A simple example to demonstrate the difference between local and
# persistent attributes.
#
# Here, local_counter acts like a standard class attribute and each
# instance has its own local copy. The persistent_counter is shared
# among all instances of the same path. These instances can be running
# in difference processes. Access to the persistent_counter is always
# transactional.

class MyObject(Object):
    
    local_counter = 0
    
    def __init__(self, path):
        # Important to call the super constructor first
        super().__init__(path)
        # The attributes method is an iterator over
        # each of the persistent attributes of an object.
        # Important to have this test in the constructor,
        # otherwise each new instance will reset the value
        # of the persistent attribute
        if 'persistent_counter' not in self.attributes():
            self.persistent_counter = 0
    
    def increment(self):
        self.local_counter += 1
        self.persistent_counter += 1
        
    def __str__(self):
        return 'local: {0} persistent {1}'.format(
            self.local_counter, self.persistent_counter
        )
```

## Objects Are Referenced By Paths


```python
# Create an instance of MyObject
obj1 = MyObject('example/obj')
```


```python
# The identifier for obj is the path example/obj1. Any instance
# constructed with that path will share the same state. The path
# is divided into the namespace example and name obj1. A namespace
# is a transactional context. 
obj1.path, obj1.namespace, obj1.name
```




    ('example/obj', 'example', 'obj')




```python
from parkit import namespaces
# List all the namespaces
list(namespaces())
```




    ['example']




```python
from parkit import objects
# List all of the objects within a single namespace
list(objects('example'))
```




    [('example/obj',
      {'databases': [],
       'versioned': True,
       'created': '2021-05-23T09:54:17.469237',
       'type': '__main__.MyObject',
       'custom': {}})]



## Objects of the Same Path Share the Same State


```python
# Create another local instance using the same
# path.
obj2 = MyObject('example/obj')
```


```python
# obj1 and obj2 are not the same Python objects
id(obj1), id(obj2)
```




    (2970709158352, 2970709159072)




```python
# But they reference the same persistent state
# which at a low-level is identified by a UUID.
obj1.uuid, obj2.uuid
```




    ('57b447c3-d79e-4ff5-8997-a87f6a296725',
     '57b447c3-d79e-4ff5-8997-a87f6a296725')




```python
# Update the obj1 counters, but not obj2
obj1.increment()
# Note the peristent counter changed for both objects
# but the local counter only changed for obj1
print(obj1, obj2)
```

    local: 1 persistent 1 local: 0 persistent 1
    

## Objects Can Have Versions


```python
# An object can be versioned. If it is, the version
# is updated after every transaction that changes the
# state commits.
obj1.versioned, obj1.version
```




    (True, 2)




```python
# The version number can also be manually updated
version = obj1.version
obj1.increment_version()
print(version, obj1.version)
```

    2 3
    

## Objects Are Modified in Transactions


```python
# An implicit transaction every time a persistent attribute
# is accessed. However, explicit transactions are also possible.
from parkit import transaction

# Update obj1 and obj2 in the same transaction.
with transaction(obj1):
    obj1.increment()
    obj2.increment()
    
print(obj1, obj2)
```

    local: 2 persistent 3 local: 1 persistent 3
    


```python
# Read-only transactions are also possible. The 
# advantage of a read-only transaction is that 
# readers never block, even if another process is
# modifying the object(s).
from parkit import snapshot

# Note we only have to pass one object to the context
# manager. Once a transaction is started, any object
# from the same namespace can be included. There is only
# one active write transaction in a namespace at any given
# time, but there can be many read transactions. Thus, 
# namespaces are used to increase write concurrency.
with snapshot(obj1):
    print(obj1, obj2)
    
# Transactions can also nest, and its possible to access 
# data in a transaction with zero-copy semantics.
```

    local: 2 persistent 3 local: 1 persistent 3
    

## Deleting Objects 


```python
# To permanently remove an object, use the drop()
# method.
obj1.drop()
obj2.drop()
```


```python
# Check the exists property to see if the object still
# exists.
obj1.exists
```




    False




```python
# Future access with result in an ObjectNotFoundError
obj1.version
```


    ---------------------------------------------------------------------------

    ObjectNotFoundError                       Traceback (most recent call last)

    <ipython-input-18-442e108d1d2f> in <module>
          1 # Future access with result in an ObjectNotFoundError
    ----> 2 obj1.version
    

    ~\Documents\Github\parkit\parkit\storage\entity.py in version(self)
        362                 txn.commit()
        363         except BaseException as exc:
    --> 364             self.__abort(exc, txn, False)
        365         finally:
        366             if txn and cursor:
    

    ~\Documents\Github\parkit\parkit\storage\entity.py in __abort(self, exc_value, txn, check_exists)
        231         if isinstance(exc_value, lmdb.Error):
        232             raise TransactionError() from exc_value
    --> 233         raise exc_value
        234 
        235     @property
    

    ~\Documents\Github\parkit\parkit\storage\entity.py in version(self)
        358                 version = struct.unpack('@N', version)[0]
        359             else:
    --> 360                 raise ObjectNotFoundError()
        361             if txn:
        362                 txn.commit()
    

    ObjectNotFoundError: 



```python

```
