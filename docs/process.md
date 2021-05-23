# Process
*parkit* provides a Process class with a similiar interface to the Python Process class in the *multiprocessing* package. One key difference is that the *parkit* process is a Unix-like daemon process and will stay running even if the parent process that launched it terminates. Another key difference is that a *parkit* process is a sub-class of Dict, so other processes can share data with the process object via the Dict iterface.

Internally, the Process class manages a pool of processes, similar to a thread pool. When a process is started, it will run on the next available worker process in the pool. The pool size defaults to the maximum of 4 or reported number of CPUs. Users can change the pool size at any time. 


```python
from parkit import (
    Log,
    Process
)
```


```python
# Define a new Process class for this example. The process will generate
# some values and write them to a shared log.
class MyProcess(Process):
    
    def run(self):
        log = self['results']
        for i in range(5):
            log.append(('iteration', i))
        return True
```

## Process Management


```python
# Check the current pool size
Process.get_pool_size()
```




    4




```python
# Set a new pool size
Process.set_pool_size(4)
Process.get_pool_size()
```




    4




```python
# Process state follows this state transition diagram
# created -> submitted -> running -> [finished, failed, crashed]
# State moves to 'submitted' when a process is started. A process ends in
# one of three states: finished, failed, or crashed. Finished means the process
# exited normally. Failed means the process exited with an exception, and
# crashed means the process terminated abnormally.
#
# We can check for processes in any combination of states

# Check for running processes
list(Process.dir())
```




    []




```python
# Check for failed, finished, or crashed processes
list(Process.dir(['finished', 'failed', 'crashed']))
```




    []




```python
# Remove all processes in a certain state
Process.clean(['failed', 'crashed'])
```


```python
# The process pool is automatically started when the first process is started.
# Thereafter the pool will continue to run until the machine is reboot. The
# killall method will shutdown the pool.
Process.killall()
```

## Process Example


```python
# Create a shared log to hold the results 
log = Log('example/log')
log.clear()
```


```python
# Create and start some process instances. Each process
# receives a reference to the shared log for results. 
procs = []
for i in range(10):
    p = MyProcess('process' + str(i))
    p['results'] = log
    p.start()
    procs.append(p)
```


```python
# Wait for all of the processes to finish. If
# the pool is already started, this should execute 
# immediately, which is one advantage of using a pool.
[proc.join() for proc in procs]
```




    [None, None, None, None, None, None, None, None, None, None]




```python
# Verify there are 50 results in the log
len(log)
```




    200




```python
# Print the first ten results
list(log[0:10])
```




    [('iteration', 0),
     ('iteration', 1),
     ('iteration', 2),
     ('iteration', 3),
     ('iteration', 4),
     ('iteration', 0),
     ('iteration', 1),
     ('iteration', 2),
     ('iteration', 3),
     ('iteration', 4)]




```python
# Go through the processes and check the return value and
# final status
for process in Process.dir(['finished', 'failed', 'crashed']):
    print(process.name, process.status, process.result)
```

    process0 finished True
    process9 finished True
    process2 finished True
    process5 finished True
    process6 finished True
    process1 finished True
    process4 finished True
    process7 finished True
    process8 finished True
    process3 finished True
    


```python
# Remove the processes
Process.clean('finished')
```


```python
# Shutdown the pool to free resources
Process.killall()
```
