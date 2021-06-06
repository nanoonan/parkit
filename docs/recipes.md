## Sharing Code Libraries Between Jupyter Notebooks


```python
%reload_ext autoreload
%autoreload 2
```


```python
import parkit as pk
```


```python
# For this example, assume this code is executed in notebook 1.
#
# Create task. Add a metadata tag for the task's 'library'. We'll make the
# task run synchronously by default.
@pk.task(
    name = 'mysum',
    metadata = dict(library = 'math'),
    default_sync = True
)
def __sum__(input: pk.Array):
    return sum(input)
```


```python
input = pk.Array()
input.extend([1, 2, 3, 4, 5])
pk.bind_task('mysum')(input)
```




    15




```python
# This code runs in notebook 2.
#
# Define a function that imports our pre-defined tasks and binds them
# to symbols in the notebook's module.

def import_library(name: str):
    print('importing library', name)
    for task in pk.Namespace('task'):
        if 'library' in task.metadata and task.metadata['library'] == name:
            if pk.bind_symbol(task.namespace, task.name, overwrite = True):
                print('bound symbol:', task.name)
```


```python
import_library('math')
```

    importing library math
    bound symbol: mysum
    


```python
input = pk.Array()
input.extend([1, 2, 3, 4, 5])
mysum(input)
```




    15




```python
# Go back to notebook 1.
#
# Let's change the default mode to run asynchronously.
@pk.task(
    name = 'mysum',
    metadata = dict(library = 'math'),
    default_sync = False
)
def __sum__(input: pk.Array):
    return sum(input)
```


```python
# Back to notebook 2.
input = pk.Array()
input.extend([1, 2, 3, 4, 5])
trace = mysum(input)
pk.wait_until(lambda: trace.done)
print(trace)
```

    AsyncTrace
    task: task/mysum
    status: finished
    pid: 5736
    start: 2021-06-04 15:43:00.788537
    end: 2021-06-04 15:43:00.819536
    


```python
trace.result
```




    15



## Zero-copy Access To Numpy Array in Shared Memory


```python
# We'll extend Dict to support serialization of Numpy arrays without
# pickling.
import numpy as np

class NumpyDict(pk.Dict):
    
    # This is called during the encoding phase and allows us to 
    # record metadata that we can use when decoding an object.
    def get_metadata(self, value):
        if isinstance(value, np.ndarray):
            return dict(
                shape = value.shape,
                dtype = value.dtype
            )
        return None
    
    # If we have metadata we know its a Numpy array. Return a zerop-copy version
    # if we are in an explicit transaction and we are passed a memoryview. Otherwise
    # create an array with a copy of the data.
    def decode_value(self, bytestring, metadata):
        if metadata is None:
            return super().decode_value(bytestring)
        if self.is_transaction and isinstance(bytestring, memoryview):
            return np.ndarray(metadata['shape'], metadata['dtype'], buffer = bytestring)
        return np.ndarray(metadata['shape'], metadata['dtype'], buffer = bytearray(bytestring))

    # If we have a numpy array, send back the bytes data, otherwise call the standard
    # pickle encoder
    def encode_value(self, value):
        if isinstance(value, np.ndarray):
            return value.data
        return super().encode_value(value)
```


```python
d = NumpyDict()
```


```python
data = np.zeros((10, 10))
```


```python
# We now have a dictionary with special (non-pickled) encoding for Numpy arrays.
d['numpy'] = data
d['other'] = [1, 2, 3]
```


```python
# Standard pickle encoding
d['other']
```




    [1, 2, 3]




```python
# The array is a zero-copy version of the data stored in shared memory.
with pk.snapshot(d.namespace):
    array = d['numpy']
    print(array)
```

    [[0. 0. 0. 0. 0. 0. 0. 0. 0. 0.]
     [0. 0. 0. 0. 0. 0. 0. 0. 0. 0.]
     [0. 0. 0. 0. 0. 0. 0. 0. 0. 0.]
     [0. 0. 0. 0. 0. 0. 0. 0. 0. 0.]
     [0. 0. 0. 0. 0. 0. 0. 0. 0. 0.]
     [0. 0. 0. 0. 0. 0. 0. 0. 0. 0.]
     [0. 0. 0. 0. 0. 0. 0. 0. 0. 0.]
     [0. 0. 0. 0. 0. 0. 0. 0. 0. 0.]
     [0. 0. 0. 0. 0. 0. 0. 0. 0. 0.]
     [0. 0. 0. 0. 0. 0. 0. 0. 0. 0.]]
    


```python
# This code will throw an exception since the array is a read-only zero-copy
# version.
with pk.snapshot(d.namespace):
    array = d['numpy']
    array[0] = 1
```


    ---------------------------------------------------------------------------

    ValueError                                Traceback (most recent call last)

    <ipython-input-18-2c73857330bb> in <module>
          3 with pk.snapshot(d.namespace):
          4     array = d['numpy']
    ----> 5     array[0] = 1
    

    ValueError: assignment destination is read-only



```python
# We can get writable copy of the array by setting the zero_copy parameter to
# False.
with pk.snapshot(d.namespace, zero_copy = False):
    array = d['numpy']
    array[0] = 1
```


```python
# Retrieving the data in an implicit transaction will always return a writable
# array with a copy of the data.
array = d['numpy']
array[0] = 1
```
