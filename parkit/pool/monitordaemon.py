# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import platform
import sys
import uuid

import daemoniker

import parkit.constants as constants

from parkit.adapters import Queue
from parkit.functions import get_pool_size
from parkit.pool.commands import (
    create_pid_filepath,
    launch_node,
    scan_nodes
)
from parkit.utility import (
    create_string_digest,
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    pid_filepath = None
    node_uid = None
    cluster_uid = None

    try:

        with daemoniker.Daemonizer() as (is_setup, daemonizer):

            if is_setup:
                node_uid = sys.argv[1]
                cluster_uid = sys.argv[2]
                pid_filepath = create_pid_filepath(node_uid, cluster_uid)

            is_parent, node_uid, cluster_uid, pid_filepath = \
            daemonizer(
                pid_filepath, node_uid, cluster_uid, pid_filepath
            )

            if is_parent:
                pass

        if platform.system() == 'Windows':
            del os.environ['__INVOKE_DAEMON__']

        polling_interval = getenv(constants.MONITOR_POLLING_INTERVAL_ENVNAME, float)

        process_queue = Queue(constants.TASK_QUEUE_PATH)

        termination_queue = Queue(constants.NODE_TERMINATION_QUEUE_PATH)

        for _ in polling_loop(polling_interval):
            try:
                pre_scan_termination_count = len(termination_queue)
                running_nodes = scan_nodes(cluster_uid if cluster_uid else 'default')
                post_scan_termination_count = len(termination_queue)

                worker_nodes = [
                    node_uid for node_uid, _ in running_nodes \
                    if node_uid not in ['monitor', 'scheduler']
                ]

                pool_size = get_pool_size()

                pre_scan_delta = pool_size - (len(worker_nodes) - pre_scan_termination_count)
                post_scan_delta = pool_size - (len(worker_nodes) - post_scan_termination_count)

                if post_scan_delta > 0:
                    for _ in range(post_scan_delta):
                        launch_node(
                            create_string_digest(str(uuid.uuid4())),
                            'parkit.pool.workerdaemon',
                            cluster_uid if cluster_uid else 'default'
                        )
                elif pre_scan_delta < 0:
                    for _ in range(abs(pre_scan_delta)):
                        termination_queue.put_nowait(True)

                if 'scheduler' not in [node_uid for node_uid, _ in running_nodes]:
                    launch_node(
                        'scheduler',
                        'parkit.pool.scheddaemon',
                        cluster_uid if cluster_uid else 'default'
                    )
            except Exception:
                logger.exception('(monitor) error on pid %i', os.getpid())

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(monitor) fatal error on pid %i', os.getpid())
