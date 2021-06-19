# pylint: disable = unpacking-non-sequence, protected-access
import logging

from typing import (
    List, Optional
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.asyncexecution import AsyncExecution
from parkit.adapters.dict import Dict
from parkit.adapters.queue import Queue
from parkit.exceptions import SiteNotSpecifiedError
from parkit.node import (
    is_running,
    launch_node,
    terminate_all_nodes
)
from parkit.storage.context import transaction_context
from parkit.storage.environment import get_environment_threadsafe
from parkit.storage.namespace import Namespace
from parkit.storage.site import get_storage_path
from parkit.system.counter import get_counter_value
from parkit.system.pidtable import get_pidtable_snapshot
from parkit.utility import getenv

logger = logging.getLogger(__name__)

def get_concurrency(*, site_uuid: Optional[str] = None) -> int:
    state = Dict(constants.CLUSTER_STATE_DICT_PATH, site_uuid = site_uuid)
    _, env, _, _, _, _ = get_environment_threadsafe(
        state.storage_path, state.namespace, create = False
    )
    with transaction_context(env, write = True):
        if 'concurrency' not in state:
            state['concurrency'] = getenv(constants.CLUSTER_CONCURRENCY_ENVNAME, int)
    return state['concurrency']

def set_concurrency(
    value: int,
    /, *,
    site_uuid: Optional[str] = None
):
    if value < 1:
        raise ValueError()
    state = Dict(constants.CLUSTER_STATE_DICT_PATH, site_uuid = site_uuid)
    state['concurrency'] = value

def enable_tasks(*, site_uuid: Optional[str] = None):
    if site_uuid is None:
        if thread.local.default_site is not None:
            storage_path, site_uuid = thread.local.default_site
        else:
            raise SiteNotSpecifiedError()
    else:
        storage_path = get_storage_path(site_uuid)
    if not is_monitor_running(site_uuid):
        launch_node(
            '-'.join(['monitor', str(get_counter_value(site_uuid))]),
            'parkit.system.monitordaemon',
            site_uuid,
            {
                constants.DEFAULT_SITE_PATH_ENVNAME: storage_path
            }
        )

def disable_tasks(*, site_uuid: Optional[str] = None):
    if site_uuid is None:
        if thread.local.default_site is not None:
            _, site_uuid = thread.local.default_site
        else:
            raise SiteNotSpecifiedError()
    terminate_all_nodes(site_uuid, priority_filter = ['monitor'])

def is_monitor_running(cluster_uid: str) -> bool:

    snapshot = get_pidtable_snapshot()

    monitor_nodes = sorted([
        (pid, entry['node_uid'].split('-')[1], entry['node_uid']) \
        for pid, entry in snapshot.items() \
        if isinstance(entry['node_uid'], str) and \
        entry['node_uid'].split('-')[0] == 'monitor' and \
        isinstance(entry['cluster_uid'], str) and \
        entry['cluster_uid'] == cluster_uid
    ], key = lambda x: x[1])

    for pid, _, node_uid in monitor_nodes:
        if is_running(node_uid, pid):
            return True
    return False

def task_executions(
    status_filter: Optional[List[str]] = None,
    /, *,
    site_uuid: Optional[str] = None
):
    if status_filter is None or any(
        True for status in status_filter if status in ['crashed', 'failed', 'finished']
    ):
        for obj in Namespace(
            constants.EXECUTION_NAMESPACE, site_uuid = site_uuid,
            create = True
        ):
            if isinstance(obj, AsyncExecution):
                if status_filter is None or obj.status in status_filter:
                    yield obj
    else:
        if 'submitted' in status_filter:
            for obj in Queue(constants.SUBMIT_QUEUE_PATH, site_uuid = site_uuid)._iterator():
                assert isinstance(obj, AsyncExecution)
                yield obj
        if 'running' in status_filter:
            for obj in Dict(constants.RUNNING_DICT_PATH, site_uuid = site_uuid).values():
                assert isinstance(obj, AsyncExecution)
                if obj.status == 'running':
                    yield obj
