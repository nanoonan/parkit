## Basics
Here are some basic examples with the Collection classes.


```python
# Set the storage path to tmp
import os
os.environ['PARKIT_STORAGE_PATH'] = 'C:\\tmp'
```


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
# Objects can be versioned.
d1.versioned, d1.version
```




    (True, 0)




```python
d1['key'] = 'value'
d1.version
```




    1




```python
# Multiple objects in the same namespace can be modified in
# a single transaction.
d2 = Dict('examples/mydict2')
with transaction(d1):
    d2['key'] = d1['key']
    d1['key'] = 'new value'
d1['key'], d2['key']
```




    ('new value', 'value')




```python
# Read-only transactions are also supported.
with snapshot(d1):
    print(d1['key'], d2['key'])
```

    new value value
    


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
      {'databases': [['7975c4a25d20f97d1cc8bc1029e0bc52335cdc11', {}]],
       'versioned': True,
       'created': '2021-05-22T15:06:48.803499',
       'type': 'parkit.adapters.dict.Dict',
       'custom': {}}),
     ('examples/mydict2',
      {'databases': [['bd30b1119c5a1776067a11aa1df86e9715ab449e', {}]],
       'versioned': True,
       'created': '2021-05-22T14:03:14.567382',
       'type': 'parkit.adapters.dict.Dict',
       'custom': {}})]




```python
# Delete an object
d1.drop()
list(objects('examples'))
```




    [('examples/mydict2',
      {'databases': [['bd30b1119c5a1776067a11aa1df86e9715ab449e', {}]],
       'versioned': True,
       'created': '2021-05-22T14:03:14.567382',
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
    
