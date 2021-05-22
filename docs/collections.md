Here are some basic examples with the Collection classes. The classes available are Dict, Log, and Queue.


```python
from parkit import (
    Dict,
    Log,
    namespaces,
    objects,
    Queue,
    transaction,
    snapshot
)
```


```python
# Create a Dict (implements standard Python Dict interface).
# The path is examples/mydict. The namespace is examples
# and the name is mydict1 for the object.
d1 = Dict('examples/mydict1')
```


```python
d1.namespace, d1.name
```




    ('examples', 'mydict1')




```python
d1['key1'] = 'value1'
d1['key2'] = 'value2'
list(d1.keys())
```




    ['key1', 'key2']




```python
# Multiple objects in the same namespace can be modified in
# a single transaction.
d2 = Dict('examples/mydict2')
with transaction(d1):
    d2['key1'] = d1['key1']
    d1['key1'] = 'new value'
d1['key1'], d2['key1']
```




    ('new value', 'value1')




```python
# Objects can be versioned.
d1.versioned, d1.version
```




    (True, 3)




```python
# All changes within a transaction count as a single version.
with transaction(d1):
    d1['key1'] = True
    d2['key2'] = False
d1.version
```




    4




```python
# Read-only transactions are also supported.
with snapshot(d1):
    print(d1['key1'], d2['key1'])
```

    True value1
    


```python
# List all namespaces
list(namespaces())
```




    ['examples']




```python
# list all objects in a namespace. Returns (path, descriptor) tuple
list(objects('examples'))
```




    [('examples/mydict1',
      {'databases': [['51dd203c7527e09e4ea0b5d92101a64160aeb1c2', {}]],
       'versioned': True,
       'created': '2021-05-22T17:59:35.167536',
       'type': 'parkit.adapters.dict.Dict',
       'custom': {}}),
     ('examples/mydict2',
      {'databases': [['13ce98ec9358fdbc761b61448ce9353d33b682e5', {}]],
       'versioned': True,
       'created': '2021-05-22T17:59:39.422523',
       'type': 'parkit.adapters.dict.Dict',
       'custom': {}})]




```python
# Delete an object
d1.drop()
list(objects('examples'))
```




    [('examples/mydict2',
      {'databases': [['13ce98ec9358fdbc761b61448ce9353d33b682e5', {}]],
       'versioned': True,
       'created': '2021-05-22T17:59:39.422523',
       'type': 'parkit.adapters.dict.Dict',
       'custom': {}})]




```python
# A Log is an append-only list
log = Log('examples/mylog')
log.clear()
log.append(1)
log.append(2)
log.append(3)
list(log)
```




    [1, 2, 3]




```python
list(log[0:2])
```




    [1, 2]




```python
# Queue is also available
queue = Queue('examples/myqueue')
queue.clear()
queue.put_nowait(1)
queue.put_nowait(2)
for _ in range(len(queue)):
    print(queue.get_nowait())
```

    1
    2
    
