# pylint: disable = c-extension-no-member, dangerous-default-value, unused-argument, invalid-name
import enum
import logging
import platform

from typing import (
    Iterator, List, Optional, Union
)

import parkit.constants as constants

from parkit.adapters.dict import Dict
from parkit.adapters.process import Process
from parkit.pool.commands import (
    terminate_all_nodes
)
from parkit.storage import objects
from parkit.utility import (
    create_string_digest,
    getenv
)

if platform.system() == 'Windows':
    import win32api
    import win32con
    import win32process

logger = logging.getLogger(__name__)

class ProcessPriority(enum.Enum):
    Low = 0
    Normal = 1
    High = 2

if platform.system() == 'Windows':
    def set_process_priority(priority: ProcessPriority):
        pid = win32api.GetCurrentProcessId()
        handle = win32api.OpenProcess(win32con.PROCESS_ALL_ACCESS, True, pid)
        if priority.value == ProcessPriority.Low.value:
            win32process.SetPriorityClass(handle, win32process.BELOW_NORMAL_PRIORITY_CLASS)
        elif priority.value == ProcessPriority.Normal.value:
            win32process.SetPriorityClass(handle, win32process.NORMAL_PRIORITY_CLASS)
        elif priority.value == ProcessPriority.High.value:
            win32process.SetPriorityClass(handle, win32process.ABOVE_NORMAL_PRIORITY_CLASS)
else:
    def set_process_priority(priority: ProcessPriority):
        pass

def processes(
    status_filter: Optional[Union[str, List[str]]] = None
) -> Iterator[Process]:
    paths = [path for path, _ in objects(constants.PROCESS_NAMESPACE)]
    for path in paths:
        process = Process(path, typecheck = False)
        if status_filter is None or \
        (isinstance(status_filter, list) and process.status in status_filter) or \
        process.status == status_filter:
            yield process

def killall():
    cluster_uid = create_string_digest(getenv(constants.STORAGE_PATH_ENVNAME))
    terminate_all_nodes(cluster_uid)

def get_pool_size() -> int:
    state = Dict(constants.PROCESS_STATE_PATH)
    if 'pool_size' not in state:
        state['pool_size'] = getenv(constants.PROCESS_POOL_SIZE_ENVNAME, int)
    return state['pool_size']

def set_pool_size(size: int):
    state = Dict(constants.PROCESS_STATE_PATH)
    state['pool_size'] = size

def clean(
    status_filter: Optional[Union[str, List[str]]] = ['finished', 'crashed', 'failed']
):
    paths = [path for path, _ in objects(constants.PROCESS_NAMESPACE)]
    for path in paths:
        process = Process(path, typecheck = False)
        if status_filter is None or \
        (isinstance(status_filter, list) and process.status in status_filter) or \
        process.status == status_filter:
            process.drop()
