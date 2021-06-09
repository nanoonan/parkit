# pylint: disable = protected-access
import logging
import os
import uuid

from typing import (
    List, Optional
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.dict import Dict
from parkit.cluster import (
    scan_nodes,
    launch_node,
    terminate_all_nodes
)
from parkit.exceptions import SiteNotSpecifiedError
from parkit.storage.context import transaction_context
from parkit.storage.site import (
    get_site_name,
    get_site_uuid
)
from parkit.storage.threadlocal import StoragePath
from parkit.system.pidtable import get_pidtable_snapshot
from parkit.utility import getenv

logger = logging.getLogger(__name__)

def acquire_daemon_lock(daemon_type: str, pool_state: Dict):
    snapshot = get_pidtable_snapshot()
    with transaction_context(pool_state._Entity__env, write = True):
        if daemon_type not in pool_state:
            pool_state[daemon_type] = (
                os.getpid(),
                getenv(constants.PROCESS_UUID_ENVNAME, str)
            )
            return True
        daemon_pid, daemon_uuid = pool_state[daemon_type]
        if daemon_pid in snapshot and snapshot[daemon_pid][1] == daemon_uuid:
            return False
        pool_state[daemon_type] = (
            os.getpid(),
            getenv(constants.PROCESS_UUID_ENVNAME, str)
        )
        return True

class Pool():

    def __init__(self, site: Optional[str] = None):
        if site:
            self._site_uuid = get_site_uuid(site)
        elif thread.local.storage_path:
            self._site_uuid = thread.local.storage_path.site_uuid
        else:
            raise SiteNotSpecifiedError()
        self._state = Dict(
            constants.POOL_STATE_DICT_PATH,
            site = site
        )

    @property
    def site(self) -> str:
        return get_site_name(self._site_uuid)

    @property
    def site_uuid(self) -> str:
        return self._site_uuid

    @property
    def size(self) -> int:
        if 'pool_size' not in self._state:
            self._state['pool_size'] = getenv(constants.POOL_SIZE_ENVNAME, int)
        return self._state['pool_size']

    @size.setter
    def size(self, value: int):
        assert value > 0
        self._state['pool_size'] = value

    @property
    def nodes(self) -> List[str]:
        return scan_nodes(self._site_uuid)

    @property
    def started(self) -> bool:
        return len(scan_nodes(self._site_uuid)) > 0

    def start(self):
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        if 'monitor' not in [node_uid.split('-')[0] for node_uid in scan_nodes(self._site_uuid)]:
            launch_node(
                'monitor-{0}'.format(str(uuid.uuid4())),
                'parkit.system.monitordaemon',
                self._site_uuid,
                {
                    constants.CLUSTER_STORAGE_PATH_ENVNAME: storage_path,
                    constants.CLUSTER_SITE_UUID_ENVNAME: self._site_uuid
                }
            )

    def stop(self):
        terminate_all_nodes(self._site_uuid)
