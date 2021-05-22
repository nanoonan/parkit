
from parkit import (
    snapshot,
    syslog
)

length = len(syslog)
print('welcome to syslog')
print('starting at index:', length)
while True:
    syslog.wait(length)
    with snapshot(syslog):
        for record in syslog[length:]:
            print(record)
        length = len(syslog)
