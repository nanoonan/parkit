import os
import uuid

from typing import cast

import parkit.constants as constants

from parkit.profiles import get_lmdb_profiles
from parkit.utility import (
    checkenv,
    envexists,
    getenv,
    setenv
)

setenv(
    constants.PROCESS_UUID_ENVNAME,
    str(uuid.uuid4())
)

if envexists(constants.STORAGE_PATH_ENVNAME):
    setenv(
        constants.STORAGE_PATH_ENVNAME,
        os.path.abspath(getenv(constants.STORAGE_PATH_ENVNAME, str))
    )

for name, default in get_lmdb_profiles()['default'].copy().items():
    if envexists(name):
        if checkenv(name, type(default)):
            cast(dict, get_lmdb_profiles())['default'][name] = getenv(name, type(default))

if not envexists(constants.POOL_SIZE_ENVNAME):
    setenv(constants.POOL_SIZE_ENVNAME, str(constants.DEFAULT_POOL_SIZE))

if not envexists(constants.MONITOR_POLLING_INTERVAL_ENVNAME):
    setenv(
        constants.MONITOR_POLLING_INTERVAL_ENVNAME,
        str(constants.DEFAULT_MONITOR_POLLING_INTERVAL)
    )

if not envexists(constants.GARBAGE_COLLECTOR_POLLING_INTERVAL_ENVNAME):
    setenv(
        constants.GARBAGE_COLLECTOR_POLLING_INTERVAL_ENVNAME,
        str(constants.DEFAULT_GARBAGE_COLLECTOR_POLLING_INTERVAL)
    )

if not envexists(constants.MONITOR_ISALIVE_INTERVAL_ENVNAME):
    setenv(
        constants.MONITOR_ISALIVE_INTERVAL_ENVNAME,
        str(constants.DEFAULT_MONITOR_ISALIVE_INTERVAL)
    )

if not envexists(constants.WORKER_POLLING_INTERVAL_ENVNAME):
    setenv(
        constants.WORKER_POLLING_INTERVAL_ENVNAME,
        str(constants.DEFAULT_WORKER_POLLING_INTERVAL)
    )

if not envexists(constants.ADAPTER_POLLING_INTERVAL_ENVNAME):
    setenv(
        constants.ADAPTER_POLLING_INTERVAL_ENVNAME,
        str(constants.DEFAULT_ADAPTER_POLLING_INTERVAL)
    )
