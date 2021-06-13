import logging

import parkit.constants as constants

from parkit.storage.site import import_site
from parkit.system.pidtable import set_pid_entry
from parkit.utility import (
    envexists,
    getenv
)

logger = logging.getLogger(__name__)

set_pid_entry()

if envexists(constants.DEFAULT_SITE_PATH) and envexists(constants.DEFAULT_SITE_NAME):
    storage_path = getenv(constants.DEFAULT_SITE_PATH, str)
    site_name = getenv(constants.DEFAULT_SITE_NAME, str)
    import_site(storage_path, name = site_name)
elif envexists(constants.DEFAULT_SITE_PATH):
    storage_path = getenv(constants.DEFAULT_SITE_PATH, str)
    import_site(storage_path)

logger.info('parkit ready')
