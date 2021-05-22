*parkit* provides a Process class with a similiar interface to the Python Process class in the *multiprocessing* package. One key difference is that the *parkit* process is Unix-like daemon process and will stay running even if the parent process that launched it terminates.


```python
from parkit import (
    Log,
    Process
)
```


```python
# Define a new Process class
class MyProcess(Process):
    
    def run(self):
        log = self['results']
        for i in range(5):
            log.append(('iteration', i))
        return True
```


```python
# We'll create a log to hold the results of our process
log = Log('examples/log')
log.clear()
```


```python
# Create a new process instance
p = MyProcess('process1')
p.uuid
```




    '498baf61-37b8-4862-9cfe-5f84e5837a55'




```python
# Every Process object is a Dict, so we can share data with the process through the Dict 
# interface. We'll assign the log object with the key 'results'.
p['results'] = log
# Start running the process in the background
p.start()
```


```python
# Check that the log received some data
len(log)
```




    5




```python
# Print the results
list(log)
```




    [('iteration', 0),
     ('iteration', 1),
     ('iteration', 2),
     ('iteration', 3),
     ('iteration', 4)]




```python
# Get the status of our process
p.status
```




    'finished'




```python
# Get the result returned by our process.
p.result
```




    True




```python
# List all the processes 
for process in Process.dir(None):
    print(process.name, process.status, process.result)
```

    process1 finished True
    


```python
# Delete any process with status of 'finished', 'crashed', or 'failed'
Process.clean()
```


```python

```
