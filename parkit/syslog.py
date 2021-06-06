# pylint: disable = invalid-name
import logging

from typing import Any

import parkit.constants as constants

from parkit.adapters.array import Array
from parkit.storage.site import import_site
from parkit.utility import getenv

import_site(
    getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str),
    name = '__global__'
)

syslog: Array = Array(
    constants.SYSLOG_PATH,
    site = '__global__'
)

class LogHandler(logging.StreamHandler):

    def emit(self, record: Any):
        syslog.append(self.format(record))

logging.basicConfig(
    format = '%(asctime)s %(levelname)s@%(name)s : %(message)s',
    level = logging.INFO,
    handlers = [LogHandler()]
)
