# pylint: disable = protected-access
import logging
import uuid

from typing import (
    List, Optional
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.dict import Dict
from parkit.cluster.manage import (
    scan_nodes,
    launch_node,
    terminate_all_nodes
)
from parkit.exceptions import SiteNotSpecifiedError
from parkit.storage.site import (
    get_site_name,
    get_site_uuid
)
from parkit.storage.threadlocal import StoragePath
from parkit.utility import (
    create_string_digest,
    getenv
)

logger = logging.getLogger(__name__)

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
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        return scan_nodes(storage_path)

    @property
    def started(self) -> bool:
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        return len(scan_nodes(storage_path)) > 0

    def start(self):
        return
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        if 'monitor' not in [node_uid.split('-')[0] for node_uid in scan_nodes(storage_path)]:
            launch_node(
                'monitor-{0}'.format(create_string_digest(str(uuid.uuid4()))),
                'parkit.cluster.monitordaemon',
                storage_path
            )

    def stop(self):
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        terminate_all_nodes(storage_path)
