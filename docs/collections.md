# Collections
The *parkit* collection classes are Dict, Log, and Queue. Generally, the classes adhere to the interfaces defined 
by Python's [Collection Abstract Bases Classes](https://docs.python.org/3/library/collections.abc.html). Dict
implements MutableMapping and Log implements Sequence. Queue maps closely to the Queue class in Python. Every *parkit*
collection is an Object, so all of the info from the previous section is relevant. Hopefully the usage of the collection
classes is largely self-explanatory. The motivation for the collection classes is to provide Python-like data structures 
usable in a multiprocessing environment.

Note that when performing multiple operations on a collection, performance improves dramatically if all of the operations
are wrapped in a single, explicit transaction.

By default, pickle is used to store the data in a collection. It's possible to provide other encoding and decoding methods
by sub-classing the relevant collection.

## Dict


```python
from parkit import Dict
```


```python
# All the goodness of a Python dict, the main difference is that Dicts are 
# named objects and the data is persistent and accessible from multiple
# processes.
d = Dict('example/mydict')
```


```python
d['key1'] = 'value1'
d['key2'] = 'value2'
list(d.keys())
```




    ['key1', 'key2']



## Log


```python
from parkit import Log
```


```python
# A Log is an append-only list. Very useful for streaming large
# numbers of objects into a persistent collection.
log = Log('example/mylog')
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



## Queue


```python
from parkit import Queue
```


```python
# Queue is also available
queue = Queue('example/myqueue')
queue.clear()
queue.put_nowait(1)
queue.put_nowait(2)
for _ in range(len(queue)):
    print(queue.get_nowait())
```

    1
    2
    


```python

```
