from typing import cast

from parkit import (
    Array,
    snapshot,
    SysLog,
    wait_until
)

log: Array = cast(Array, SysLog())

length = len(log)

print('welcome to syslog')
print('current log size:', length)

while True:
    wait_until(lambda: len(log) > length)
    with snapshot(log.namespace):
        for record in log[length:]:
            print(record)
        length = len(log)
