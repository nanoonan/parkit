# pylint: disable = invalid-name, global-statement, too-few-public-methods
import logging

from typing import (
    Any, List, Optional
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.array import Array

cache: List[str] = []

log: Optional[Array] = None

class SysLog():

    def __new__(cls):
        global log
        if log is None:
            log = Array(constants.SYSLOG_PATH)
        return log

class LogHandler(logging.StreamHandler):

    def emit(self, record: Any):
        global log
        if log is None:
            log = Array(constants.SYSLOG_PATH)
        if not thread.local.transaction:
            if cache:
                for entry in cache:
                    log.append(entry)
            cache.clear()
            log.append(self.format(record))
        else:
            cache.append(self.format(record))

logging.basicConfig(
    format = '%(asctime)s %(levelname)s@%(name)s : %(message)s',
    level = logging.INFO,
    handlers = [LogHandler()]
)
