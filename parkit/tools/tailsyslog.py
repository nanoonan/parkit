from typing import cast

import parkit.constants as constants

from parkit.storage.transaction import snapshot
from parkit.storage.wait import wait
from parkit.system import syslog
from parkit.utility import getenv

length = len(syslog)

print('welcome to syslog')
print('current log size:', length)
print('installation path:', getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str))

while True:
    with snapshot(syslog):
        for record in syslog[length:]:
            print(record)
        length = len(syslog)
    wait(syslog, lambda: len(syslog) > length)
