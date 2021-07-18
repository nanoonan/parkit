# pylint: disable = broad-except, protected-access, invalid-name
import logging
import os
import uuid

import parkit.constants as constants

from parkit.adapters.queue import Queue
from parkit.node import (
    is_running,
    launch_node,
    terminate_node
)
from parkit.storage.site import get_default_site
from parkit.system.cluster import get_concurrency
from parkit.system.pidtable import pidtable
from parkit.utility import (
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    try:
        node_uid = getenv(constants.NODE_UID_ENVNAME, str)
        cluster_uid = getenv(constants.CLUSTER_UID_ENVNAME, str)

        logger.info('monitor (%s) started for site %s', node_uid, get_default_site())

        polling_interval = getenv(constants.MONITOR_POLLING_INTERVAL_ENVNAME, float)

        termination_queue = Queue(constants.NODE_TERMINATION_QUEUE_PATH, create = True)

        for i in polling_loop(polling_interval):

            try:

                #
                # Restart nodes if needed
                #

                pre_scan_termination_count = len(termination_queue)

                snapshot = pidtable.get_snapshot()

                monitor_nodes = sorted([
                    (pid, int(entry['node_uid'].split('-')[1]), entry['node_uid']) \
                    for pid, entry in snapshot.items() \
                    if isinstance(entry['node_uid'], str) and \
                    entry['node_uid'].split('-')[0] == 'monitor' and \
                    isinstance(entry['cluster_uid'], str) and \
                    entry['cluster_uid'] == cluster_uid
                ], key = lambda entry: entry[1])

                assert node_uid in [entry[2] for entry in monitor_nodes]

                if len(monitor_nodes) > 1:
                    index = 0
                    should_exit = False
                    while True:
                        assert index < len(monitor_nodes)
                        if node_uid != monitor_nodes[index][2]:
                            if is_running(monitor_nodes[index][2], monitor_nodes[index][0]):
                                logger.info('duplicate monitor (%s) terminating', node_uid)
                                should_exit = True
                                break
                            index += 1
                            continue
                        break
                    if should_exit:
                        break

                post_scan_termination_count = len(termination_queue)

                worker_nodes = [
                    entry['node_uid'] for entry in snapshot.values() \
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
                    default_site = get_default_site()
                    assert default_site is not None
                    storage_path, _ = default_site
                    for j in range(post_scan_delta):
                        worker_node_uid = '-'.join([
                            constants.WORKER_DAEMON_MODULE.split('.')[-1],
                            str(uuid.uuid4())
                        ])
                        process_uid = str(uuid.uuid4())
                        pid = launch_node(
                            worker_node_uid,
                            constants.WORKER_DAEMON_MODULE,
                            cluster_uid,
                            {
                                constants.DEFAULT_SITE_PATH_ENVNAME: storage_path,
                                constants.PROCESS_UID_ENVNAME: process_uid
                            }
                        )
                        pidtable.set_pid_entry(
                            pid = pid, process_uid = process_uid,
                            node_uid = worker_node_uid, cluster_uid = cluster_uid
                        )
                elif pre_scan_delta < 0:
                    for j in range(abs(pre_scan_delta)):
                        termination_queue.put(True)

                scheduler_nodes = [
                    entry['node_uid'] for entry in snapshot.values() \
                    if isinstance(entry['node_uid'], str) and \
                    entry['node_uid'].split('-')[0] == 'scheduler' and \
                    isinstance(entry['cluster_uid'], str) and \
                    entry['cluster_uid'] == cluster_uid
                ]

                if not scheduler_nodes:
                    default_site = get_default_site()
                    assert default_site is not None
                    storage_path, _ = default_site
                    scheduler_node_uid = '-'.join([
                        constants.SCHEDULER_DAEMON_MODULE.split('.')[-1],
                        str(uuid.uuid4())
                    ])
                    process_uid = str(uuid.uuid4())
                    launch_node(
                        scheduler_node_uid,
                        constants.SCHEDULER_DAEMON_MODULE,
                        cluster_uid,
                        {
                            constants.DEFAULT_SITE_PATH_ENVNAME: storage_path,
                            constants.PROCESS_UID_ENVNAME: process_uid
                        }
                    )
                    pidtable.set_pid_entry(
                        pid = pid, process_uid = process_uid,
                        node_uid = scheduler_node_uid, cluster_uid = cluster_uid
                    )
                elif len(scheduler_nodes) > 1:
                    for uid in scheduler_nodes[1:]:
                        terminate_node(uid)

            except Exception:
                logger.exception('(monitor) error on pid %i', os.getpid())

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(monitor) fatal error on pid %i', os.getpid())
