# pylint: disable = unpacking-non-sequence, protected-access
import logging
import uuid

from typing import (
    List, Optional
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.asyncexecution import AsyncExecution
from parkit.adapters.dict import Dict
from parkit.exceptions import SiteNotSpecifiedError
from parkit.node import (
    launch_node,
    terminate_all_nodes
)
from parkit.storage.context import transaction_context
from parkit.storage.environment import get_environment_threadsafe
from parkit.storage.namespace import Namespace
from parkit.storage.site import get_storage_path
from parkit.system.pidtable import pidtable
from parkit.utility import getenv

logger = logging.getLogger(__name__)

def get_concurrency(*, site_uuid: Optional[str] = None) -> int:
    state = Dict(
        constants.CLUSTER_STATE_DICT_PATH, site_uuid = site_uuid,
        create = True, bind = True
    )
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
    state = Dict(
        constants.CLUSTER_STATE_DICT_PATH, site_uuid = site_uuid,
        create = True, bind = True
    )
    state['concurrency'] = value

def enable_tasks(*, site_uuid: Optional[str] = None) -> bool:
    if site_uuid is None:
        if thread.local.default_site is not None:
            storage_path, site_uuid = thread.local.default_site
        else:
            raise SiteNotSpecifiedError()
    else:
        storage_path = get_storage_path(site_uuid)
    monitor_name = constants.MONITOR_DAEMON_MODULE.split('.')[-1]
    with pidtable:
        if not any(
            True for entry in pidtable.get_snapshot().values() \
            if isinstance(entry['node_uid'], str) and \
            entry['node_uid'].split('-')[0] == monitor_name and \
            isinstance(entry['cluster_uid'], str) and \
            entry['cluster_uid'] == site_uuid
        ):
            node_uid = '-'.join([
                monitor_name,
                str(pidtable.next_counter_value()),
                str(uuid.uuid4())
            ])
            process_uid = str(uuid.uuid4())
            pid = launch_node(
                node_uid,
                constants.MONITOR_DAEMON_MODULE,
                site_uuid,
                {
                    constants.DEFAULT_SITE_PATH_ENVNAME: storage_path,
                    constants.PROCESS_UID_ENVNAME: process_uid
                }
            )
            pidtable.set_pid_entry(
                pid = pid, process_uid = process_uid,
                node_uid = node_uid, cluster_uid = site_uuid
            )
            return True
        return False

def disable_tasks(*, site_uuid: Optional[str] = None) -> bool:
    if site_uuid is None:
        if thread.local.default_site is not None:
            _, site_uuid = thread.local.default_site
        else:
            raise SiteNotSpecifiedError()
    monitor_name = constants.MONITOR_DAEMON_MODULE.split('.')[-1]
    with pidtable:
        if any(
            True for entry in pidtable.get_snapshot().values() \
            if isinstance(entry['node_uid'], str) and \
            entry['node_uid'].split('-')[0] == monitor_name and \
            isinstance(entry['cluster_uid'], str) and \
            entry['cluster_uid'] == site_uuid
        ):
            terminate_all_nodes(site_uuid, priority_filter = [monitor_name])
            return True
        return False

def task_executions(
    status_filter: Optional[List[str]] = None,
    /, *,
    site_uuid: Optional[str] = None
):
    for obj in Namespace(
        constants.EXECUTION_NAMESPACE, site_uuid = site_uuid,
        create = True
    ):
        if isinstance(obj, AsyncExecution):
            if status_filter is None or obj.status in status_filter:
                yield obj
