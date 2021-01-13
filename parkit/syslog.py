import logging

from typing import Any

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.log import Log

syslog: Log = Log(constants.SYSLOG_PATH)

class LogHandler(logging.StreamHandler):

    def emit(self, record: Any) -> None:
        if not thread.local.transaction:
            syslog.append(self.format(record))
        # else:
        #     raise RuntimeError('Cannot write log entry in transaction')

logging.basicConfig(
    format = '%(asctime)s %(levelname)s@%(name)s : %(message)s',
    level = logging.INFO,
    handlers = [LogHandler()]
)
