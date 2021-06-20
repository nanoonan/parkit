import logging
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

logger = logging.getLogger(__name__)

if not envexists(constants.PROCESS_UID_ENVNAME):
    setenv(
        constants.PROCESS_UID_ENVNAME,
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

for profile_name in get_lmdb_profiles():
    for name, default in cast(dict, get_lmdb_profiles())[profile_name].copy().items():
        if envexists('_'.join([profile_name.upper(), name])):
            if checkenv('_'.join([profile_name.upper(), name]), type(default)):
                cast(dict, get_lmdb_profiles())[profile_name][name] = \
                getenv('_'.join([profile_name.upper(), name]), type(default))

if not envexists(constants.CLUSTER_CONCURRENCY_ENVNAME):
    setenv(constants.CLUSTER_CONCURRENCY_ENVNAME, str(constants.DEFAULT_CLUSTER_CONCURRENCY))

if not envexists(constants.MAX_SYSLOG_ENTRIES_ENVNAME):
    setenv(constants.MAX_SYSLOG_ENTRIES_ENVNAME, str(constants.DEFAULT_MAX_SYSLOG_ENTRIES))

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

if not envexists(constants.SCHEDULER_HEARTBEAT_INTERVAL_ENVNAME):
    setenv(
        constants.SCHEDULER_HEARTBEAT_INTERVAL_ENVNAME,
        str(constants.DEFAULT_SCHEDULER_HEARTBEAT_INTERVAL)
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
