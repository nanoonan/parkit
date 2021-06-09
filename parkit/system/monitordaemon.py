# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import time
import uuid

import daemoniker

import parkit.constants as constants

from parkit.adapters.dict import Dict
from parkit.adapters.queue import Queue
from parkit.cluster import (
    create_pid_filepath,
    launch_node,
    scan_nodes,
    terminate_node
)
from parkit.storage.site import (
    get_site_uuid,
    import_site
)
from parkit.system.pool import acquire_daemon_lock
from parkit.utility import (
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    logger.info('start')
    node_uid = None
    cluster_uid = None
    pid_filepath = None

    try:

        with daemoniker.Daemonizer() as (is_setup, daemonizer):

            if is_setup:
                logger.info('setup')
                assert constants.NODE_UID_ENVNAME in os.environ and \
                constants.CLUSTER_UID_ENVNAME in os.environ
                node_uid = os.environ[constants.NODE_UID_ENVNAME]
                cluster_uid = os.environ[constants.CLUSTER_UID_ENVNAME]
                pid_filepath = create_pid_filepath(node_uid, cluster_uid)

            is_parent, node_uid, cluster_uid, pid_filepath = \
            daemonizer(
                pid_filepath, node_uid, cluster_uid, pid_filepath
            )

            if is_parent:
                pass

        storage_path = getenv(constants.CLUSTER_STORAGE_PATH_ENVNAME, str)
        site_uuid = getenv(constants.CLUSTER_SITE_UUID_ENVNAME, str)
        import_site(storage_path, name = 'main')
        assert get_site_uuid('main') == site_uuid

        pool_state = Dict(constants.POOL_STATE_DICT_PATH)

        if 'pool_size' not in pool_state:
            pool_state['pool_size'] = getenv(constants.POOL_SIZE_ENVNAME, int)

        if not acquire_daemon_lock('monitor', pool_state):
            logger.info('monitor failed to acquire lock...exiting (%s)', node_uid)
            assert node_uid is not None and cluster_uid is not None
            terminate_node(node_uid, cluster_uid)
            while True:
                time.sleep(1)

        logging.info('monitor started (%s)', node_uid)

        polling_interval = getenv(constants.MONITOR_POLLING_INTERVAL_ENVNAME, float)

        process_queue = Queue(constants.TASK_QUEUE_PATH)

        termination_queue = Queue(constants.NODE_TERMINATION_QUEUE_PATH)

        for _ in polling_loop(polling_interval):
            try:
                pre_scan_termination_count = len(termination_queue)
                running_nodes = scan_nodes(site_uuid)
                post_scan_termination_count = len(termination_queue)

                worker_nodes = [
                    node_uid for node_uid in running_nodes \
                    if node_uid.split('-')[0] not in ['monitor', 'scheduler']
                ]

                pool_size = pool_state['pool_size']

                pre_scan_delta = pool_size - (len(worker_nodes) - pre_scan_termination_count)
                post_scan_delta = pool_size - (len(worker_nodes) - post_scan_termination_count)

                if post_scan_delta > 0:
                    for _ in range(post_scan_delta):
                        assert cluster_uid is not None
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

                if 'scheduler' not in [node_uid.split('-')[0] for node_uid in running_nodes]:
                    assert cluster_uid is not None
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
