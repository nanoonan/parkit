# pylint: disable = protected-access
import logging
import uuid

from typing import (
    List, Optional
)

import parkit.constants as constants

from parkit.adapters import Dict
from parkit.cluster.manage import (
    scan_nodes,
    launch_node,
    terminate_all_nodes
)
from parkit.functions import wait_until
from parkit.storage import resolve_storage_path
from parkit.utility import (
    create_string_digest,
    getenv
)

logger = logging.getLogger(__name__)

class Pool():

    def __init__(self, storage_path: Optional[str] = None):
        self._storage_path = resolve_storage_path(storage_path)
        self._cluster_uid = create_string_digest(self._storage_path)
        self._state = Dict(
            constants.POOL_STATE_DICT_PATH,
            storage_path = self._storage_path
        )

    @property
    def storage_path(self) -> str:
        return self._storage_path

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
        return scan_nodes(self._cluster_uid)

    @property
    def started(self) -> bool:
        return len(scan_nodes(self._cluster_uid)) > 0

    def start(self, python_names: Optional[List[str]] = None):
        if python_names:
            launch_node(
                'monitor-{0}'.format(create_string_digest(str(uuid.uuid4()))),
                'parkit.cluster.monitordaemon',
                self._cluster_uid,
                environment = {constants.PYTHON_NAMES_ENVNAME: ','.join(python_names)}
            )
        else:
            launch_node(
                'monitor-{0}'.format(create_string_digest(str(uuid.uuid4()))),
                'parkit.cluster.monitordaemon',
                self._cluster_uid
            )
        wait_until(
            lambda: 'monitor' in [
                node_uid.split('-')[0]
                for node_uid in  scan_nodes(self._cluster_uid)
            ]
        )

    def stop(self):
        terminate_all_nodes(self._cluster_uid)
