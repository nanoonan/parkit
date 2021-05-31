import logging

from typing import (
    Any, List
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.array import Array

syslog: Array = Array(constants.SYSLOG_PATH)

cache: List[str] = []

class LogHandler(logging.StreamHandler):

    def emit(self, record: Any):
        if not thread.local.transaction:
            if cache:
                for entry in cache:
                    syslog.append(entry)
            cache.clear()
            syslog.append(self.format(record))
        else:
            cache.append(self.format(record))

logging.basicConfig(
    format = '%(asctime)s %(levelname)s@%(name)s : %(message)s',
    level = logging.INFO,
    handlers = [LogHandler()]
)
