# Process
*parkit* provides a *Process* class with a similiar interface to the [Process](https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Process) class in the [multiprocessing](https://docs.python.org/3/library/multiprocessing.html) package. One key difference is that the *parkit* process is a Unix-like daemon process and will stay running even if the parent process that launched it terminates. Another key difference is that a *parkit* process is a sub-class of *Dict*, so other processes can share data with the process object via the *Dict* iterface.

Internally, the *Process* class manages a pool of processes, similar to a thread pool. When a process is started, it will run on the next available worker process in the pool. The pool size defaults to the maximum of 4 or the reported number of CPUs. Users can change the pool size at any time. 

## Basic Process Usage


```python
import logging

from parkit import (
    clean,
    Process,
    processes,
    syslog
)

logger = logging.getLogger(__name__)
```

To create an anonymous process, just pass a target function and optional arguments and keyword arguments to *Process*. Call the *start* method to run the process.


```python
def do_something(arg):
    logger.info('called do_something with {0}'.format(arg))
    return 'result from do_something'

Process(target = do_something, args = ('hello',)).start()
```


```python
# logger by default writes to syslog. Check last entry in syslog.
syslog[-1]
```




    '2021-05-25 14:54:29,024 INFO@__main__ : called do_something with hello'



The *processes* function returns an iterator over the process table. The process table contains every process instance.


```python
[(p.name, p.exitcode, p.result, p.status) for p in processes()]
```




    [('process-653b7f0a-e842-41b8-8d19-3806ecf60b89',
      0,
      'result from do_something',
      'finished')]



Note the status is finished.  Process status follows this state transition diagram:

    created -> submitted -> running -> (finished, failed, crashed)

Status moves to submitted when a process is started. A process ends in
one of three statuses: finished, failed, or crashed. Finished means the process
exited normally. Failed means the process exited with an exception, and
crashed means the process terminated abnormally.

The *clean* function removes processes from the process table. By default, *clean* removes all processes with status of finished, failed, or crashed.


```python
clean()
```


```python
[(p.name, p.exitcode, p.result, p.status) for p in processes()]
```




    []



A process can also have an explicit path identifier.


```python
Process('example/myprocess', target = do_something, args = ('hello',)).start()
```


```python
[(p.name, p.exitcode, p.result, p.status) for p in processes()]
```




    [('example/myprocess', 0, 'result from do_something', 'finished')]




```python
clean()
```

## Custom Process Class

You can also sub-class the *Process* class and override the *run* method.


```python
from parkit import Log
```


```python
# Define a new Process class. The process will generate some values and 
# write them to a shared results log. The results log will be passed to
# the process using the dictionary interface.
class MyProcess(Process):
    
    def run(self):
        log = self['results']
        for i in range(5):
            log.append(('iteration', i))
        return True
```


```python
# Create a shared log to hold the results.
log = Log('example/log')
log.clear()
```


```python
# Create and start ten process instances. Each process receives a reference 
# to the shared results log. 
procs = []
for i in range(10):
    p = MyProcess()
    p['results'] = log
    p.start()
    procs.append(p)
```


```python
# Wait for all of the processes to finish. 
[p.join() for p in procs]
```




    [None, None, None, None, None, None, None, None, None, None]




```python
# Verify there are 50 results in the log
len(log)
```




    50




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
[(p.name, p.exitcode, p.result, p.status) for p in processes()]
```




    [('process-541c7dd8-5f97-4b9e-96fc-b08884d430ac', 0, True, 'finished'),
     ('process-42e78473-78d6-4d39-8c92-e1e7cfdf3490', 0, True, 'finished'),
     ('process-7a4b8b4b-7aa6-4282-a0b7-cc15c3ded659', 0, True, 'finished'),
     ('process-7dd38446-521f-47fc-a501-2c4df6189f81', 0, True, 'finished'),
     ('process-728e4b95-4c08-4c20-bc54-6b947c71437c', 0, True, 'finished'),
     ('process-19c62544-0333-4ca3-bdef-2f52996b1491', 0, True, 'finished'),
     ('process-9894d3f1-3804-46eb-a030-c4d3ab2470de', 0, True, 'finished'),
     ('process-a9f1b3e0-d193-4f42-a65f-de388dbb6002', 0, True, 'finished'),
     ('process-4544ac66-d079-459e-858b-90d8e3bcf8f7', 0, True, 'finished'),
     ('process-fd97ae7b-cdae-400e-8d2e-8c5d28c6bb44', 0, True, 'finished')]




```python
clean()
```

## Pool Management


```python
from parkit import (
    get_pool_size,
    killall,
    set_pool_size
)
```

The process pool size is dynamic.


```python
# Check the current pool size
get_pool_size()
```




    4




```python
# Set a new pool size
set_pool_size(6)
get_pool_size()
```




    6



The process pool is automatically started when the first process is started. Thereafter the pool will continue to run until the machine is rebooted. The *killall* function will shutdown the pool and release the pool resources. Processes running when *killall* is invoked are forcefully terminated and will report a crashed status. 


```python
killall()
```
