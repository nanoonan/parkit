import logging

import parkit.constants as constants

from parkit.storage.site import set_default_site
from parkit.system.pidtable import pidtable
from parkit.utility import (
    envexists,
    getenv
)

logger = logging.getLogger(__name__)

if envexists(constants.DEFAULT_SITE_PATH_ENVNAME):
    set_default_site(
        getenv(constants.DEFAULT_SITE_PATH_ENVNAME, str),
        create = True
    )

pidtable.set_pid_entry()
