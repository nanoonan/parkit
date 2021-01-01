import logging
import multiprocessing
import os
import tempfile

from typing import (
    cast, Optional
)

import parkit.constants as constants

from parkit.pool import (
    launch_node,
    scan_nodes,
    terminate_all_nodes
)
from parkit.profiles import get_lmdb_profiles
from parkit.utility import (
    checkenv,
    create_string_digest,
    envexists,
    getenv,
    setenv
)

logger = logging.getLogger(__name__)

def is_pool_started() -> bool:
    cluster_uid = create_string_digest(getenv(constants.INSTALL_PATH_ENVNAME))
    return len(scan_nodes(cluster_uid)) > 0

def start_pool(
    size: int = multiprocessing.cpu_count(),
    monitor_polling_interval: float = constants.DEFAULT_MONITOR_POLLING_INTERVAL,
    tasker_polling_interval: float = constants.DEFAULT_TASKER_POLLING_INTERVAL
) -> bool:
    cluster_uid = create_string_digest(getenv(constants.INSTALL_PATH_ENVNAME))
    running = scan_nodes(cluster_uid)
    if [node_uid for node_uid, _ in running if node_uid == 'monitor']:
        return False
    launch_node(
        'monitor',
        'parkit.pool.monitordaemon',
        cluster_uid, monitor_polling_interval, tasker_polling_interval, size
    )
    return True

def stop_pool() -> bool:
    cluster_uid = create_string_digest(getenv(constants.INSTALL_PATH_ENVNAME))
    terminate_all_nodes(cluster_uid)
    return True

def _set_environment(install_path: Optional[str] = None) -> None:
    path = cast(
        str,
        os.getenv(constants.INSTALL_PATH_ENVNAME) if not install_path else install_path
    )
    path = os.path.abspath(path)
    if os.path.exists(path):
        if not os.path.isdir(path):
            raise ValueError('Install_path is not a directory')
    else:
        os.makedirs(path)
    for name, default in get_lmdb_profiles()['persistent'].copy().items():
        if envexists(name):
            if checkenv(name, type(default)):
                cast(dict, get_lmdb_profiles())['persistent'][name] = getenv(name, type(default))
    setenv(constants.INSTALL_PATH_ENVNAME, path)

if envexists(constants.INSTALL_PATH_ENVNAME):
    _set_environment(install_path = getenv(constants.INSTALL_PATH_ENVNAME))
else:
    try:
        os.makedirs(
            os.path.join(tempfile.gettempdir(), constants.PARKIT_TEMP_INSTALLATION_DIRNAME)
        )
    except FileExistsError:
        pass
    _set_environment(
        install_path = os.path.join(
            tempfile.gettempdir(), constants.PARKIT_TEMP_INSTALLATION_DIRNAME
        )
    )
