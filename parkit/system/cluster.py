import logging
import uuid

from typing import (
    List, Optional
)

import parkit.constants as constants

from parkit.adapters.dict import Dict
from parkit.exceptions import SiteNotSpecifiedError
from parkit.node import (
    launch_node,
    terminate_all_nodes
)
from parkit.storage.context import transaction_context
from parkit.storage.site import (
    get_site,
    get_site_uuid
)
from parkit.storage.threadlocal import StoragePath
from parkit.system.pidtable import get_pidtable_snapshot
from parkit.utility import getenv

logger = logging.getLogger(__name__)

def get_concurrency(*, site: Optional[str] = None) -> int:
    state = Dict(constants.CLUSTER_STATE_DICT_PATH, site = site)
    with transaction_context(state._Entity__env, write = True):
        if 'concurrency' not in state:
            state['concurrency'] = getenv(constants.CLUSTER_CONCURRENCY_ENVNAME, int)
    return state['concurrency']

def set_concurrency(value: int, /, *, site: Optional[str] = None):
    if value < 1:
        raise ValueError()
    state = Dict(constants.CLUSTER_STATE_DICT_PATH, site = site)
    state['concurrency'] = value

def enable_tasks(*, site: Optional[str] = None):
    if site is None:
        site = get_site()
        if site is None:
            raise SiteNotSpecifiedError()
    site_uuid = get_site_uuid(site)
    snapshot = get_pidtable_snapshot()
    if len([
        entry['node_uid'] for entry in snapshot.values() \
        if isinstance(entry['node_uid'], str) and \
        entry['node_uid'].split('-')[0] == 'monitor' and \
        isinstance(entry['cluster_uid'], str) and \
        entry['cluster_uid'] == site_uuid
    ]) == 0:
        storage_path = StoragePath(site_uuid = site_uuid).path
        launch_node(
            'monitor-{0}'.format(str(uuid.uuid4())),
            'parkit.system.monitordaemon',
            site_uuid,
            {
                constants.CLUSTER_STORAGE_PATH_ENVNAME: storage_path,
                constants.CLUSTER_SITE_UUID_ENVNAME: site_uuid
            }
        )

def disable_tasks(*, site: Optional[str] = None):
    if site is None:
        site = get_site()
        if site is None:
            raise SiteNotSpecifiedError()
    terminate_all_nodes(get_site_uuid(site))

def running(*, site: Optional[str] = None) -> List[str]:
    return []
