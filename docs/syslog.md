# Syslog
The installed logger writes log entries to a system log. The log is accessible through the *syslog* variable. 
A utility program, parkit.tools.tailsyslog, can be run from the command line to tail the entries written to
*syslog*.


```python
from parkit import syslog
```


```python
len(syslog)
```




    128226




```python
list(syslog[-5:])
```




    ['2021-05-23 11:03:17,007 ERROR@__main__ : Task daemon caught user error\nTraceback (most recent call last):\n  File "c:\\users\\rdpuser\\appdata\\local\\programs\\python\\python38\\lib\\site-packages\\parkit\\pool\\taskdaemon.py", line 70, in <module>\n    result = process.run()\n  File "<ipython-input-2-170c5ea4e1e0>", line 6, in run\n  File "c:\\users\\rdpuser\\appdata\\local\\programs\\python\\python38\\lib\\site-packages\\parkit\\adapters\\dict.py", line 135, in __getitem__\n    raise KeyError()\nKeyError',
     '2021-05-23 11:03:17,017 ERROR@__main__ : Task daemon caught user error\nTraceback (most recent call last):\n  File "c:\\users\\rdpuser\\appdata\\local\\programs\\python\\python38\\lib\\site-packages\\parkit\\pool\\taskdaemon.py", line 70, in <module>\n    result = process.run()\n  File "<ipython-input-2-170c5ea4e1e0>", line 6, in run\n  File "c:\\users\\rdpuser\\appdata\\local\\programs\\python\\python38\\lib\\site-packages\\parkit\\adapters\\dict.py", line 135, in __getitem__\n    raise KeyError()\nKeyError',
     '2021-05-23 11:03:17,026 ERROR@__main__ : Task daemon caught user error\nTraceback (most recent call last):\n  File "c:\\users\\rdpuser\\appdata\\local\\programs\\python\\python38\\lib\\site-packages\\parkit\\pool\\taskdaemon.py", line 70, in <module>\n    result = process.run()\n  File "<ipython-input-2-170c5ea4e1e0>", line 6, in run\n  File "c:\\users\\rdpuser\\appdata\\local\\programs\\python\\python38\\lib\\site-packages\\parkit\\adapters\\dict.py", line 135, in __getitem__\n    raise KeyError()\nKeyError',
     '2021-05-23 11:03:17,035 ERROR@__main__ : Task daemon caught user error\nTraceback (most recent call last):\n  File "c:\\users\\rdpuser\\appdata\\local\\programs\\python\\python38\\lib\\site-packages\\parkit\\pool\\taskdaemon.py", line 70, in <module>\n    result = process.run()\n  File "<ipython-input-2-170c5ea4e1e0>", line 6, in run\n  File "c:\\users\\rdpuser\\appdata\\local\\programs\\python\\python38\\lib\\site-packages\\parkit\\adapters\\dict.py", line 135, in __getitem__\n    raise KeyError()\nKeyError',
     '2021-05-23 11:03:17,043 ERROR@__main__ : Task daemon caught user error\nTraceback (most recent call last):\n  File "c:\\users\\rdpuser\\appdata\\local\\programs\\python\\python38\\lib\\site-packages\\parkit\\pool\\taskdaemon.py", line 70, in <module>\n    result = process.run()\n  File "<ipython-input-2-170c5ea4e1e0>", line 6, in run\n  File "c:\\users\\rdpuser\\appdata\\local\\programs\\python\\python38\\lib\\site-packages\\parkit\\adapters\\dict.py", line 135, in __getitem__\n    raise KeyError()\nKeyError']




```python
import logging
logger = logging.getLogger(__name__)
logger.info('Generated entry in syslog')
syslog[-1]
```




    '2021-05-23 12:27:38,031 INFO@__main__ : Generated entry in syslog'


