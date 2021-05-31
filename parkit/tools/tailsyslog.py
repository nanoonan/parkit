
from parkit import (
    snapshot,
    syslog,
    wait_until
)

length = len(syslog)
print('welcome to syslog')
print('current log size:', length)
while True:
    wait_until(lambda: len(syslog) > length)
    with snapshot(syslog.namespace):
        for record in syslog[length:]:
            print(record)
        length = len(syslog)
