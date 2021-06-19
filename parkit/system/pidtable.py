import logging
import os
import typing

from typing import (
    Optional, Union
)

import psutil

import parkit.constants as constants

from parkit.adapters.dict import Dict
from parkit.storage.context import transaction_context
from parkit.storage.environment import get_environment_threadsafe
from parkit.storage.site import (
    get_site_uuid,
    import_site
)
from parkit.utility import (
    envexists,
    getenv
)
logger = logging.getLogger(__name__)

import_site(getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str), create = True)

pid_table = Dict(
    constants.PID_TABLE_DICT_PATH,
    site_uuid = get_site_uuid(getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str))
)

def set_pid_entry():
    pid = os.getpid()
    pid_table[pid] = dict(
        create_time = psutil.Process(pid).create_time(),
        process_uuid = getenv(constants.PROCESS_UUID_ENVNAME, str),
        node_uid = getenv(constants.NODE_UID_ENVNAME, str) \
        if envexists(constants.NODE_UID_ENVNAME) else None,
        cluster_uid = getenv(constants.CLUSTER_UID_ENVNAME, str) \
        if envexists(constants.CLUSTER_UID_ENVNAME) else None
    )

def get_pidtable_snapshot() -> typing.Dict[int, typing.Dict[str, Union[float, Optional[str]]]]:
    _, env, _, _, _, _ = get_environment_threadsafe(
        pid_table.storage_path, pid_table.namespace,
        create = False
    )
    with transaction_context(env, write = True):
        active_pids = []
        recorded_pids = list(pid_table.keys())
        for proc in psutil.process_iter(['create_time', 'pid']):
            try:
                pid = proc.info['pid']
                active_pids.append(pid)
                if pid in recorded_pids:
                    create_time = pid_table[pid]['create_time']
                    if create_time < proc.info['create_time']:
                        del pid_table[pid]
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        for pid in set(recorded_pids).difference(set(active_pids)):
            del pid_table[pid]
        return dict(pid_table)
