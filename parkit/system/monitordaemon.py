# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import uuid

import parkit.constants as constants

from parkit.adapters.queue import Queue
from parkit.adapters.synchronized import synchronized
from parkit.node import launch_node
from parkit.storage.site import (
    get_site_uuid,
    import_site
)
from parkit.system.cluster import get_concurrency
from parkit.system.pidtable import get_pidtable_snapshot
from parkit.utility import (
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    try:

        node_uid = getenv(constants.NODE_UID_ENVNAME, str)
        cluster_uid = getenv(constants.CLUSTER_UID_ENVNAME, str)
        storage_path = getenv(constants.CLUSTER_STORAGE_PATH_ENVNAME, str)
        site_uuid = getenv(constants.CLUSTER_SITE_UUID_ENVNAME, str)
        import_site(storage_path, name = 'main')
        assert get_site_uuid('main') == site_uuid

        logging.info('monitor started (%s)', node_uid)

        polling_interval = getenv(constants.MONITOR_POLLING_INTERVAL_ENVNAME, float)

        process_queue = Queue(constants.EXECUTION_QUEUE_PATH)

        termination_queue = Queue(constants.NODE_TERMINATION_QUEUE_PATH)

        for _ in polling_loop(polling_interval):

            try:

                with synchronized(cluster_uid):

                    pre_scan_termination_count = len(termination_queue)

                    snapshot = get_pidtable_snapshot()

                    post_scan_termination_count = len(termination_queue)

                    worker_nodes = [
                        entry['node_uid'] for pid, entry in snapshot.items() \
                        if isinstance(entry['node_uid'], str) and \
                        entry['node_uid'].split('-')[0] == 'worker' and \
                        isinstance(entry['cluster_uid'], str) and \
                        entry['cluster_uid'] == cluster_uid
                    ]

                    concurrency = get_concurrency()

                    pre_scan_delta = \
                    concurrency - (len(worker_nodes) - pre_scan_termination_count)

                    post_scan_delta = \
                    concurrency - (len(worker_nodes) - post_scan_termination_count)

                    if post_scan_delta > 0:
                        for _ in range(post_scan_delta):
                            launch_node(
                                'worker-{0}'.format(str(uuid.uuid4())),
                                'parkit.system.workerdaemon',
                                cluster_uid,
                                {
                                    constants.CLUSTER_STORAGE_PATH_ENVNAME: storage_path,
                                    constants.CLUSTER_SITE_UUID_ENVNAME: site_uuid
                                }
                            )
                    elif pre_scan_delta < 0:
                        for _ in range(abs(pre_scan_delta)):
                            termination_queue.put(True)

                    scheduler_nodes = [
                        entry['node_uid'] for pid, entry in snapshot.items() \
                        if isinstance(entry['node_uid'], str) and \
                        entry['node_uid'].split('-')[0] == 'scheduler' and \
                        isinstance(entry['cluster_uid'], str) and \
                        entry['cluster_uid'] == cluster_uid
                    ]

                    if not scheduler_nodes:
                        launch_node(
                            'scheduler-{0}'.format(str(uuid.uuid4())),
                            'parkit.system.scheddaemon',
                            cluster_uid,
                            {
                                constants.CLUSTER_STORAGE_PATH_ENVNAME: storage_path,
                                constants.CLUSTER_SITE_UUID_ENVNAME: site_uuid
                            }
                        )

            except Exception:
                logger.exception('(monitor) error on pid %i', os.getpid())

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(monitor) fatal error on pid %i', os.getpid())
