#
# reviewed: 6/14/21
#
import logging
import os

import parkit.constants as constants

from parkit.storage.site import set_default_site
from parkit.system.pidtable import set_pid_entry
from parkit.utility import (
    envexists,
    getenv
)

logger = logging.getLogger(__name__)

if envexists(constants.DEFAULT_SITE_PATH_ENVNAME):
    storage_path = getenv(constants.DEFAULT_SITE_PATH_ENVNAME, str)
    logger.info('set default path on pid=%i to %s', os.getpid(), storage_path)
    set_default_site(storage_path, create = True)

set_pid_entry()
