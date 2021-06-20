import argparse

import parkit.constants as constants

from parkit.storage.transaction import snapshot
from parkit.storage.wait import wait
from parkit.system.syslog import syslog
from parkit.utility import getenv

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description = 'Print syslog entries')

    parser.add_argument('--level')

    args = parser.parse_args()

    levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

    if args.level is not None:
        levels = levels[levels.index(args.level):]

    with snapshot(syslog):
        version = syslog.version
        index = len(syslog)

    print('welcome to syslog')
    print('installation path:', getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str))
    if syslog.maxsize is None:
        print('syslog length is unbounded')
    else:
        print('syslog holds a maximum of', syslog.maxsize, 'entries')
    while True:
        wait(syslog, lambda: syslog.version > version)
        with snapshot(syslog):
            n_new_entries = syslog.version - version
            if syslog.maxsize is None:
                for _ in range(n_new_entries):
                    record = syslog[index]
                    index += 1
                    if any(''.join([level, '@']) in record for level in levels):
                        print(record)
            else:
                while index < syslog.maxsize:
                    if n_new_entries == 0:
                        break
                    record = syslog[index]
                    n_new_entries -= 1
                    index += 1
                    if any(''.join([level, '@']) in record for level in levels):
                        print(record)
                for i in range(syslog.maxsize - n_new_entries, syslog.maxsize):
                    record = syslog[i]
                    if any(''.join([level, '@']) in record for level in levels):
                        print(record)
            version = syslog.version
