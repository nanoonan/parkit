# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import platform
import time
import uuid

import daemoniker
import psutil

import parkit.constants as constants

from parkit.adapters import (
    Dict,
    Queue
)
from parkit.cluster.manage import (
    create_pid_filepath,
    launch_node,
    scan_nodes,
    terminate_node
)
from parkit.storage import (
    get_storage_path,
    transaction
)
from parkit.threads import garbage_collector
from parkit.utility import (
    create_string_digest,
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    node_uid = None
    cluster_uid = None
    pid_filepath = None

    try:

        with daemoniker.Daemonizer() as (is_setup, daemonizer):

            if is_setup:
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

        if platform.system() == 'Windows':
            os.environ['__MONITOR_DAEMON__'] = 'True'
            if '__INVOKE_DAEMON__' in os.environ:
                del os.environ['__INVOKE_DAEMON__']

        assert cluster_uid == create_string_digest(get_storage_path())

        pool_state = Dict(constants.POOL_STATE_DICT_PATH)

        def acquire():
            with transaction(pool_state.namespace):
                if 'monitor' not in pool_state:
                    pool_state['monitor'] = (
                        os.getpid(),
                        getenv(constants.PROCESS_UUID_ENVNAME, str)
                    )
                    return True
                monitor_pid, monitor_uuid = pool_state['monitor']
                if monitor_pid == os.getpid() and \
                monitor_uuid == getenv(constants.PROCESS_UUID_ENVNAME, str):
                    return True
                try:
                    process = psutil.Process(monitor_pid)
                    if constants.PROCESS_UUID_ENVNAME in process.environ():
                        if monitor_uuid == process.environ()[constants.PROCESS_UUID_ENVNAME]:
                            return False
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                pool_state['monitor'] = (
                    os.getpid(),
                    getenv(constants.PROCESS_UUID_ENVNAME, str)
                )
                return True

        if not acquire():
            logger.info('monitor failed to acquire...exiting (%s)', node_uid)
            assert node_uid is not None
            terminate_node(node_uid, cluster_uid)
            while True:
                time.sleep(1)

        logging.info('monitor started (%s)', node_uid)

        garbage_collector.start()

        polling_interval = getenv(constants.MONITOR_POLLING_INTERVAL_ENVNAME, float)

        process_queue = Queue(constants.TASK_QUEUE_PATH)

        termination_queue = Queue(constants.NODE_TERMINATION_QUEUE_PATH)

        for _ in polling_loop(polling_interval):
            try:
                pre_scan_termination_count = len(termination_queue)
                running_nodes = scan_nodes(cluster_uid)
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
                        launch_node(
                            'worker-{0}'.format(create_string_digest(str(uuid.uuid4()))),
                            'parkit.cluster.workerdaemon',
                            cluster_uid
                        )
                elif pre_scan_delta < 0:
                    for _ in range(abs(pre_scan_delta)):
                        termination_queue.put_nowait(True)

                if 'scheduler' not in [node_uid.split('-')[0] for node_uid in running_nodes]:
                    launch_node(
                        'scheduler-{0}'.format(create_string_digest(str(uuid.uuid4()))),
                        'parkit.cluster.scheddaemon',
                        cluster_uid
                    )
            except Exception:
                logger.exception('(monitor) error on pid %i', os.getpid())

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(monitor) fatal error on pid %i', os.getpid())
