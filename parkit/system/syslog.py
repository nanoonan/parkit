import logging

from typing import Any

import parkit.constants as constants

from parkit.adapters.array import Array
from parkit.storage.site import (
    get_site_uuid,
    import_site
)
from parkit.utility import getenv

import_site(getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str), create = True)

syslog: Array = Array(
    constants.SYSLOG_PATH,
    site_uuid = get_site_uuid(getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str))
)

class LogHandler(logging.StreamHandler):

    def emit(self, record: Any):
        syslog.append(self.format(record))

logging.basicConfig(
    format = '%(asctime)s %(levelname)s@%(name)s : %(message)s',
    level = logging.INFO,
    handlers = [LogHandler()]
)
