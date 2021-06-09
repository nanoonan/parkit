import os
import tempfile
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

if not envexists(constants.GLOBAL_FILE_LOCK_PATH_ENVNAME):
    path = os.path.abspath(os.path.join(
        tempfile.gettempdir(),
        constants.GLOBAL_FILE_LOCK_FILENAME
    ))
    setenv(constants.GLOBAL_FILE_LOCK_PATH_ENVNAME, path)

if not envexists(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME):
    path = os.path.abspath(os.path.join(
        tempfile.gettempdir(),
        constants.PARKIT_TEMP_SITE_DIRNAME
    ))
    setenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, path)
else:
    path = os.path.abspath(getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str))
    setenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, path)

for name, default in get_lmdb_profiles()['default'].copy().items():
    if envexists(name):
        if checkenv(name, type(default)):
            cast(dict, get_lmdb_profiles())['default'][name] = getenv(name, type(default))

if not envexists(constants.POOL_SIZE_ENVNAME):
    setenv(constants.POOL_SIZE_ENVNAME, str(constants.DEFAULT_POOL_SIZE))

if not envexists(constants.PROCESS_TERMINATION_TIMEOUT_ENVNAME):
    setenv(
        constants.PROCESS_TERMINATION_TIMEOUT_ENVNAME,
        str(constants.DEFAULT_PROCESS_TERMINATION_TIMEOUT)
    )

if not envexists(constants.MONITOR_POLLING_INTERVAL_ENVNAME):
    setenv(
        constants.MONITOR_POLLING_INTERVAL_ENVNAME,
        str(constants.DEFAULT_MONITOR_POLLING_INTERVAL)
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

if not envexists(constants.RESTARTER_POLLING_INTERVAL_ENVNAME):
    setenv(
        constants.RESTARTER_POLLING_INTERVAL_ENVNAME,
        str(constants.DEFAULT_RESTARTER_POLLING_INTERVAL)
    )
