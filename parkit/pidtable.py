import logging
import os

from typing import (
    List, Tuple
)

import psutil

import parkit.constants as constants

from parkit.adapters.dict import Dict
from parkit.storage.context import transaction_context
from parkit.storage.site import import_site
from parkit.utility import getenv

logger = logging.getLogger(__name__)

import_site(
    getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str),
    name = '__global__'
)

pid_table = Dict(
    constants.PID_TABLE_DICT_PATH,
    site = '__global__'
)

def set_pid_entry():
    pid_table[os.getpid()] = (
        psutil.Process(os.getpid()).create_time(),
        getenv(constants.PROCESS_UUID_ENVNAME, str)
    )
    logger.info('set pid %i to entry %s', os.getpid(), str(pid_table[os.getpid()]))

def get_pidtable_snapshot() -> List[Tuple[int, Tuple[float, str]]]:
    with transaction_context(pid_table._Entity__env, write = True):
        active_pids = []
        recorded_pids = list(pid_table.keys())
        for proc in psutil.process_iter(['create_time', 'pid']):
            try:
                pid = proc.info['pid']
                active_pids.append(pid)
                if pid in recorded_pids:
                    create_time, _ = pid_table[pid]
                    if create_time < proc.info['create_time']:
                        del pid_table[pid]
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        for pid in set(recorded_pids).difference(set(active_pids)):
            del pid_table[pid]
        return list(pid_table.items())
