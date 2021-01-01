
from parkit import (
    snapshot,
    syslog,
    timestamp
)

length = len(syslog)
while True:
    syslog.wait(length)
    with snapshot(syslog):
        for record in syslog[length:]:
            print(record)
        length = len(syslog)
