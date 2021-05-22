import logging
import os

from typing import cast

import parkit.constants as constants

from .profiles import get_lmdb_profiles
from .utility import (
    checkenv,
    envexists,
    getenv,
    setenv
)

logger = logging.getLogger(__name__)

if envexists(constants.STORAGE_PATH_ENVNAME):
    setenv(
        constants.STORAGE_PATH_ENVNAME,
        os.path.abspath(getenv(constants.STORAGE_PATH_ENVNAME))
    )

for name, default in get_lmdb_profiles()['default'].copy().items():
    if envexists(name):
        if checkenv(name, type(default)):
            cast(dict, get_lmdb_profiles())['default'][name] = getenv(name, type(default))

if not envexists(constants.PROCESS_POOL_SIZE_ENVNAME):
    setenv(constants.PROCESS_POOL_SIZE_ENVNAME, str(constants.DEFAULT_PROCESS_POOL_SIZE))

if not envexists(constants.MONITOR_POLLING_INTERVAL_ENVNAME):
    setenv(
        constants.MONITOR_POLLING_INTERVAL_ENVNAME,
        str(constants.DEFAULT_MONITOR_POLLING_INTERVAL)
    )

if not envexists(constants.TASKER_POLLING_INTERVAL_ENVNAME):
    setenv(
        constants.TASKER_POLLING_INTERVAL_ENVNAME,
        str(constants.DEFAULT_TASKER_POLLING_INTERVAL)
    )

if not envexists(constants.ADAPTER_POLLING_INTERVAL_ENVNAME):
    setenv(
        constants.ADAPTER_POLLING_INTERVAL_ENVNAME,
        str(constants.DEFAULT_ADAPTER_POLLING_INTERVAL)
    )
