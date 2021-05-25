# Collections
The *parkit* collection classes are *Dict*, *Log*, and *Queue*. Generally, the classes adhere to the interfaces defined 
by Python's [Collections Abstract Base Classes](https://docs.python.org/3/library/collections.abc.html). *Dict*
implements [MutableMapping](https://docs.python.org/3/library/collections.abc.html) and 
*Log* implements [Sequence](https://docs.python.org/3/library/collections.abc.html). *Queue*
generally follows Python's [Queue](https://docs.python.org/3/library/queue.html#queue.Queue) interface.

Every *parkit* collection is an *Object*, so all of the info from the previous section is relevant. 

Every operation defined on a collection class runs in a transaction. By default an implicit transaction is started, but explicit transactions are also supported. Note that when performing multiple operations on a collection, performance improves dramatically if all of the operations are wrapped in a single, explicit transaction.

By default, pickle is used to store the data in a collection. It's possible to provide other encoding and decoding methods
by sub-classing the relevant class.

## Dict


```python
from parkit import Dict
```


```python
d1 = Dict('example/mydict')
```


```python
d1['key1'] = 'value1'
d1['key2'] = 'value2'
list(d1.keys())
```




    ['key1', 'key2']




```python
d1.clear()
```

*Dict*, *Log*, and *Queue* sub-class *Object* and inherit its functionality.


```python
d1.path
```




    'example/mydict'




```python
d1.versioned, d1.version
```




    (True, 3)




```python
d1.descriptor
```




    {'databases': [['45d885f592ef4d1b534a201fe61a55c27620b523', {}]],
     'versioned': True,
     'created': '2021-05-25T16:24:55.708542',
     'type': 'parkit.adapters.dict.Dict',
     'custom': {}}




```python
d1.uuid
```




    '443121c2-751b-41dc-a0e2-75f86d0b4642'




```python
d1.my_attribute = 'a persistent attribute'
```


```python
d2 = Dict('example/mydict')
```


```python
d2.my_attribute
```




    'a persistent attribute'




```python
d1.drop()
d2.drop()
```

## Log


```python
from parkit import Log
```


```python
log = Log('example/mylog')

log.append(1)
log.append(2)
log.append(3)
```


```python
list(log)
```




    [1, 2, 3]




```python
list(log[0:2])
```




    [1, 2]




```python
log.drop()
```

## Queue


```python
from parkit import Queue
```


```python
queue = Queue('example/myqueue')

queue.put_nowait(1)
queue.put_nowait(2)
```


```python
for _ in range(len(queue)):
    print(queue.get_nowait())
```

    1
    2
    


```python
queue.drop()
```
