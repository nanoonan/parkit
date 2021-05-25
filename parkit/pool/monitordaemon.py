# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import platform
import sys
import time
import uuid

import daemoniker
import psutil

import parkit.constants as constants

from parkit.adapters import (
    get_pool_size,
    Process
)
from parkit.pool.commands import (
    create_pid_filepath,
    launch_node,
    scan_nodes,
    terminate_node
)
from parkit.exceptions import log
from parkit.storage import (
    objects,
    transaction
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

        for _ in polling_loop(polling_interval):
            running_nodes = scan_nodes(cluster_uid if cluster_uid else 'default')
            tasker_nodes = [node_uid for node_uid, _ in running_nodes if node_uid != 'monitor']
            n_estimated_nodes = len(tasker_nodes)
            pool_size = get_pool_size()
            if n_estimated_nodes < pool_size:
                for _ in range(pool_size - n_estimated_nodes):
                    launch_node(
                        create_string_digest(str(uuid.uuid4())),
                        'parkit.pool.taskdaemon',
                        cluster_uid if cluster_uid else 'default'
                    )
            elif n_estimated_nodes > pool_size:
                exclude = []
                with transaction(constants.PROCESS_NAMESPACE):
                    paths = [path for path, _ in objects(constants.PROCESS_NAMESPACE)]
                    for path in paths:
                        process = Process(path, typecheck = False)
                        if process.status == 'running':
                            exclude.append(process._Process__get('node_uid'))
                    count = 0
                    pids = []
                    for node_uid in tasker_nodes:
                        if node_uid not in exclude:
                            pids.append(
                                terminate_node(node_uid, cluster_uid if cluster_uid else 'default')
                            )
                            count += 1
                            if count >= (n_estimated_nodes - pool_size):
                                break
                    for pid in pids:
                        if pid is not None:
                            while psutil.pid_exists(pid):
                                time.sleep(0)

    except Exception as exc:
        log(exc)
