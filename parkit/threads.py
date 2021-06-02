# pylint: disable = broad-except
import logging
import threading
import time
import uuid

import parkit.constants as constants

from parkit.cluster.manage import (
    launch_node,
    scan_nodes
)
from parkit.storage import (
    get_storage_path,
    Namespace
)
from parkit.utility import (
    create_string_digest,
    getenv,
    polling_loop,
    scan_python_processes
)

logger = logging.getLogger(__name__)

def monitor_restarter_run():
    time.sleep(getenv(constants.MONITOR_RESTARTER_POLLING_INTERVAL_ENVNAME, float))
    for _ in polling_loop(getenv(constants.MONITOR_RESTARTER_POLLING_INTERVAL_ENVNAME, float)):
        try:
            assert get_storage_path() is not None
            cluster_uid = create_string_digest(get_storage_path())
            if 'monitor' not in [node_uid.split('-')[0] for node_uid in scan_nodes(cluster_uid)]:
                launch_node(
                    'monitor-{0}'.format(create_string_digest(str(uuid.uuid4()))),
                    'parkit.cluster.monitordaemon',
                    cluster_uid
                )
        except Exception:
            logger.exception('monitor restarter error')

monitor_restarter = threading.Thread(target = monitor_restarter_run, daemon = True)

def garbage_collector_run():
    for _ in polling_loop(getenv(constants.GARBAGE_COLLECTOR_POLLING_INTERVAL_ENVNAME, float)):
        try:
            logger.info('garbage collector started')
            active = {uuid for _, uuid, _, _ in scan_python_processes()}
            for namespace in [
                Namespace(constants.DEFAULT_NAMESPACE),
                Namespace(constants.TASK_NAMESPACE)
            ]:
                orphans = []
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

garbage_collector = threading.Thread(target = garbage_collector_run, daemon = True)
