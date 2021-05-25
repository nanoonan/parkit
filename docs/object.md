# Object
Every class in *parkit* derives from *Object*. The *Object* class represents an object with persistent attributes. It's easy to sub-class *Object* to create custom persistent objects. An instance of *Object* is accessible from multiple processes. Access to the persistent attributes occurs in either an implicit or explicit transaction.

## Example


```python
from parkit import Object

# A simple example to demonstrate the difference between local and persistent 
# attributes.
#
# Here, local_counter acts like a typical object attribute and each instance 
# has its own local copy. The persistent_counter is shared among all instances 
# constructed with the same path identifier. These instances can be running in 
# difference processes. Access to the persistent_counter is always transactional.

class MyObject(Object):
    
    local_counter = 0
    
    def __init__(self, path):
        
        # Important to call the super constructor first
        super().__init__(path)
        
        # The attributes method is an iterator over each of the persistent 
        # attributes of an object. Important to have this test in the 
        # constructor, otherwise each new instance will reset the value of the 
        # persistent attribute.
        if 'persistent_counter' not in self.attributes():
            self.persistent_counter = 0
    
    def increment(self):
        self.local_counter += 1
        self.persistent_counter += 1
        
    def __str__(self):
        return '{0}:{1} local_counter->{2} persistent_counter->{3}'.format(
            self.path, id(self), self.local_counter, self.persistent_counter
        )
```

## Objects Are Referenced By Paths


```python
obj1 = MyObject('example/obj')
```

The identifier for this object is the path *example/obj*. Any instance constructed with that path will share the same state. The path is divided into a namespace and a name based on the path separator '/' (just like a directory tree on a filesystem). A namespace is a transactional context. 


```python
obj1.path, obj1.namespace, obj1.name
```




    ('example/obj', 'example', 'obj')



Use the *namespaces* function to list all current namespaces.


```python
from parkit import namespaces

list(namespaces())
```




    ['example']



Use the *objects* function to list info about all objects in a namespace.


```python
from parkit import objects

list(objects('example'))
```




    [('example/obj',
      {'databases': [],
       'versioned': True,
       'created': '2021-05-25T15:48:17.632583',
       'type': '__main__.MyObject',
       'custom': {}})]



## Objects of the Same Path Share the Same State


```python
obj2 = MyObject('example/obj')
```


```python
# obj1 and obj2 are not the same Python objects
id(obj1), id(obj2)
```




    (2033004013744, 2033004013600)




```python
# But they reference the same persistent state which at a low-level is 
# identified by a UUID.
obj1.uuid, obj2.uuid
```




    ('fadef5a2-e3ad-4017-99e3-99a81acd6573',
     'fadef5a2-e3ad-4017-99e3-99a81acd6573')




```python
# Update the obj1 counters, but not obj2
obj1.increment()
```


```python
# Note the peristent counter changed for both objects but the local 
# counter only changed for obj1
print(obj1)
print(obj2)
```

    example/obj:2033004013744 local_counter->1 persistent_counter->1
    example/obj:2033004013600 local_counter->0 persistent_counter->1
    

## Objects Can Have Versions

Versioning is available for objects. If an object is versioned, the version is updated after each successfult transaction commit.


```python
obj1.versioned, obj1.version
```




    (True, 2)



It's also possible to update the version manually.


```python
version = obj1.version
obj1.increment_version()
print(version, obj1.version)
```

    2 3
    

## Objects Are Modified in Transactions
An implicit transaction is started every time a persistent attribute is accessed. However, explicit transactions are also possible.


```python
from parkit import transaction

# Update obj1 and obj2 in the same explicit transaction.
with transaction(obj1):
    obj1.increment()
    obj2.increment()
    
print(obj1) 
print(obj2)
```

    example/obj:2033004013744 local_counter->2 persistent_counter->3
    example/obj:2033004013600 local_counter->1 persistent_counter->3
    

Read-only transactions are also possible. The advantage of a read-only transaction is that readers never block, even if another process is modifying the object(s).

Note we only have to pass one object to the context manager. Once a transaction is started, any object from the same namespace can be included. 
There is only one active write transaction in a namespace at any given time, but there can be many read transactions. 
Namespaces are useful for increasing write concurrency.

Transactions can also nest, and its possible to access data in a transaction with zero-copy semantics.


```python
from parkit import snapshot

with snapshot(obj1):
    print(obj1) 
    print(obj2)
```

    example/obj:2033004013744 local_counter->2 persistent_counter->3
    example/obj:2033004013600 local_counter->1 persistent_counter->3
    

## Deleting Objects 

To permanently remove an object, use the *drop* method.


```python
obj1.drop()
obj2.drop()
```

The *exists* property returns True or False depending on whether the underlying object state was dropped.


```python
obj1.exists
```




    False


