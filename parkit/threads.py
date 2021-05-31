# pylint: disable = broad-except
import logging
import threading
import time

import parkit.constants as constants

from parkit.pool.commands import (
    launch_node,
    scan_nodes,
    scan_python_processes
)
from parkit.storage import Namespace
from parkit.utility import (
    create_string_digest,
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

def monitor_run():
    try:
        cluster_uid = create_string_digest(getenv(constants.STORAGE_PATH_ENVNAME, str))
        if 'monitor' not in [node_uid for node_uid, _ in scan_nodes(cluster_uid)]:
            launch_node(
                'monitor',
                'parkit.pool.monitordaemon',
                cluster_uid
            )
    except Exception:
        logger.exception('monitor error')

def garbage_collector_run():
    for _ in polling_loop(getenv(constants.GARBAGE_COLLECTOR_POLLING_INTERVAL_ENVNAME, float)):
        try:
            active = {uuid for _, uuid in scan_python_processes()}
            orphans = []
            namespace = Namespace(constants.DEFAULT_NAMESPACE)
            for name, descriptor in namespace.descriptors():
                if descriptor['anonymous']:
                    if descriptor['origin'] not in active:
                        orphans.append(name)
            for name in orphans:
                time.sleep(0)
                try:
                    obj = namespace[name]
                    obj.drop()
                    logger.info('garbage collected %s', name)
                except KeyError:
                    continue
        except Exception:
            logger.exception('garbage collector error')

monitor = threading.Thread(target = monitor_run, daemon = True)

garbage_collector = threading.Thread(target = garbage_collector_run, daemon = True)
