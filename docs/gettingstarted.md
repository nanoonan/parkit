## Collections


```python
%reload_ext autoreload
%autoreload 2
```


```python
import os
import sys
os.environ['PARKIT_STORAGE_PATH'] = 'C:\\Users\\rdpuser\\Desktop\\test'
os.environ['PYTHONPATH'] = os.environ['PYTHONPATH'] + 'C:\\Users\\rdpuser\\Documents\\Github\\parkit;'
sys.path.append('C:\\Users\\rdpuser\\Documents\\Github\\parkit')
```


```python
import parkit as pk
```


```python
# First need to set a path for storing the data
pk.set_storage_path('C:\\Users\\rdpuser\\Desktop\\test')
```


```python
pk.get_storage_path()
```




    'C:\\Users\\rdpuser\\Desktop\\test'




```python
# Create a persistent dictionary.
d = pk.Dict('some/namespace/mydict')
d.update(dict(key1 = 1, key2 = 2))
list(d.items())
```




    [('key1', 1), ('key2', 2)]




```python
d = None
# Object can be referenced on any process using its path
d = pk.Dict('some/namespace/mydict')
list(d.items())
```




    [('key1', 1), ('key2', 2)]




```python
# Array class, basically an append-only list
a = pk.Array('myarray')
a.extend([3, 2, 1, 4])
sum(a)
```




    10




```python
sorted(a)
```




    [1, 2, 3, 4]




```python
# Queue class follows standard Queue interface
q = pk.Queue('myqueue')
q.put(1)
q.get()
```




    1




```python
# Properties of persistent objects
print(d.path, d.name, d.versioned, d.version, d.exists, d.metadata)
```

    some/namespace/mydict mydict True 1 True {}
    


```python
# Delete the object
d.drop()
d.exists
```




    False



## Transactions


```python
a1 = pk.Array('namespace1/a1')
a1.extend([1, 2, 3, 4])
a2 = pk.Array('namespace2/a2')
a2.extend([5, 6, 7, 8])

with pk.snapshot(a1.namespace):
    a1[0] = -1
    a2[0] = -1
    print('a1 value from when snapshot opened', a1[0])
    print('a2 is in different namespace, returns current value', a2[0])
    
print('New values', a1[0], a2[0])
```

    a1 value from when snapshot opened 1
    a2 is in different namespace, returns current value -1
    New values -1 -1
    


```python
# Operations grouped in transaction commit as single change
a3 = pk.Array('namespace1/a3')
with pk.transaction(a1.namespace):
    a1[0] = -1
    a3.append(1)
    
list(a1[:]), list(a3[:])
```




    ([-1, 2, 3, 4], [1])



## Tasks


```python
import logging
logger = logging.getLogger(__name__)
```


```python
@pk.task
def cumsum(input: pk.Array, output: pk.Array):
    sum = 0
    for x in input:
        sum += x
        # This bug is deliberate
        output.append(summation)
    logger.info('finished cumsum run')
    return True
```


```python
cumsum
```




    <parkit.adapters.task.Task at 0x1cb4c26ec10>




```python
pool = pk.Pool()
```


```python
# Start a process pool for remote task execution. The pool runs independently of the
# current process and will stay running even if this process is terminated.
pool.start()
```


```python
pool.size
```




    4




```python
input = pk.Array()
output = pk.Array()
input.extend([1, 2, 3, 4, 5])
trace = cumsum(input, output)
pk.wait_until(lambda: trace.done)
list(output[:])
```




    []




```python
print(trace)
```

    AsyncTrace
    task: task/cumsum
    status: failed
    pid: 5736
    start: 2021-06-04 12:57:19.967522
    end: 2021-06-04 12:57:20.046532
    


```python
trace.error
```




    NameError("name 'summation' is not defined")




```python
@pk.task
def cumsum(input: pk.Array, output: pk.Array):
    sum = 0
    for x in input:
        sum += x
        # Bug fix in place
        output.append(sum)
    logger.info('finished cumsum run')
    return True
```


```python
input = pk.Array()
output = pk.Array()
input.extend([1, 2, 3, 4, 5])
trace = cumsum(input, output)
pk.wait_until(lambda: trace.done)
list(output[:])
```




    [1, 3, 6, 10, 15]




```python
print(trace)
```

    AsyncTrace
    task: task/cumsum
    status: finished
    pid: 5736
    start: 2021-06-04 12:57:27.107555
    end: 2021-06-04 12:57:27.153512
    


```python
input = pk.Array()
output = pk.Array()
input.extend([1, 2, 3, 4, 5])

# It's also possible to run synchronously in current process
cumsum(input, output, sync = True)
list(output[:])
```




    [1, 3, 6, 10, 15]



## Task Scheduling


```python
scheduler = pk.Periodic(
    start = 'now',
    frequency = pk.Frequency.Second,
    period = 1,
    max_times = 3
)
```


```python
input = pk.Array()
output = pk.Array()
input.extend([1, 2, 3, 4, 5])

cumsum.schedule(scheduler, input, output)
```




    True




```python
list(output[:])
```




    [1, 3, 6, 10, 15, 1, 3, 6, 10, 15, 1, 3, 6, 10, 15]




```python
# syslog captures the logger output
syslog = pk.SysLog()
list(syslog[-3:])
```




    ['2021-06-04 12:57:40,020 INFO@__main__ : finished cumsum run',
     '2021-06-04 12:57:41,003 INFO@__main__ : finished cumsum run',
     '2021-06-04 12:57:42,006 INFO@__main__ : finished cumsum run']



## Namespaces


```python
# namespace iterator
[path.path for path in pk.namespaces()]
```




    ['default', 'namespace1', 'namespace2', 'some', 'some/namespace', 'task']




```python
# object iterator
[obj.path for obj in pk.Namespace('namespace1')]
```




    ['namespace1/a1', 'namespace1/a3']




```python

```
