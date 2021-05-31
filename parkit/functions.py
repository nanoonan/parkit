import logging
import pickle

from typing import (
    Optional, Union
)

import parkit.constants as constants
import parkit.storage as storage

from parkit.adapters.dict import Dict
from parkit.pool.commands import (
    launch_node,
    scan_nodes,
    terminate_all_nodes
)
from parkit.utility import (
    create_string_digest,
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

pool_state = Dict(constants.POOL_STATE_DICT_PATH)

def self():
    try:
        return pickle.loads(getenv(constants.SELF_ENVNAME, str).encode())
    except ValueError:
        return None

def restart():
    cluster_uid = create_string_digest(getenv(constants.STORAGE_PATH_ENVNAME, str))
    terminate_all_nodes(cluster_uid)
    wait_until(lambda: not scan_nodes(cluster_uid))
    launch_node(
        'monitor',
        'parkit.pool.monitordaemon',
        cluster_uid
    )

def shutdown():
    cluster_uid = create_string_digest(getenv(constants.STORAGE_PATH_ENVNAME, str))
    terminate_all_nodes(cluster_uid)

def get_pool_size() -> int:
    if 'pool_size' not in pool_state:
        return getenv(constants.POOL_SIZE_ENVNAME, int)
    return pool_state['pool_size']

def set_pool_size(size: int):
    pool_state['pool_size'] = size

def wait_until(
    condition,
    /, *,
    interval: Optional[float] = None,
    namespace: Optional[Union[storage.Namespace, str]] = None,
    snapshot: Optional[bool] = None,
    timeout: Optional[float] = None
):
    interval = getenv(constants.ADAPTER_POLLING_INTERVAL_ENVNAME, float) \
    if interval is None else interval
    for _ in wait_for(
        condition, interval,
        namespace = namespace, snapshot = snapshot,
        timeout = timeout
    ):
        pass

def wait_for(
    condition,
    interval: float,
    /, *,
    namespace: Optional[Union[storage.Namespace, str]] = None,
    snapshot: Optional[bool] = None,
    timeout: Optional[float] = None
):
    for i in polling_loop(interval, timeout = timeout):
        if snapshot is not None:
            if snapshot:
                with storage.snapshot(namespace):
                    if condition():
                        return
            else:
                with storage.transaction(namespace):
                    if condition():
                        return
        else:
            if condition():
                return
        yield i
