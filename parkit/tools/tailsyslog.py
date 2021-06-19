import argparse

from typing import cast

import parkit.constants as constants

from parkit.storage.transaction import snapshot
from parkit.storage.wait import wait
from parkit.system import syslog
from parkit.utility import getenv

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = 'Print syslog entries')

    parser.add_argument('--level')

    args = parser.parse_args()

    levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

    if args.level is not None:
        levels = levels[levels.index(args.level):]

    length = len(syslog)

    print('welcome to syslog')
    print('installation path:', getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str))

    while True:
        with snapshot(syslog):
            for record in syslog[length:]:
                if any(''.join([level, '@']) in record for level in levels):
                    print(record)
            length = len(syslog)
        wait(syslog, lambda: len(syslog) > length)
