from typing import cast

import parkit.constants as constants

from parkit.functions import wait_until
from parkit.storage.transaction import snapshot

from parkit.syslog import syslog
from parkit.utility import getenv

length = len(syslog)

print('welcome to syslog')
print('current log size:', length)
print('installation path:', getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str))

while True:
    wait_until(lambda: len(syslog) > length)
    with snapshot(syslog.namespace):
        for record in syslog[length:]:
            print(record)
        length = len(syslog)
