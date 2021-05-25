# Syslog

The installed logger writes log entries to a system log. The log is accessible through the *syslog* variable. 
A utility program, *parkit.tools.tailsyslog*, can be run from the command line to tail the entries written to
*syslog*.


```python
from parkit import syslog
```


```python
len(syslog)
```




    128373




```python
list(syslog[-2:])
```




    ['2021-05-25 14:55:42,646 ERROR@parkit.adapters.process : process run',
     '2021-05-25 14:55:42,648 INFO@__main__ : called do_something with hello']



Generate entries in the *syslog* by instantiating a logger and calling the appropriate logging method.


```python
import logging

logger = logging.getLogger(__name__)

logger.info('Generated entry in syslog')
syslog[-1]
```




    '2021-05-25 15:06:48,634 INFO@__main__ : Generated entry in syslog'




```python

```
