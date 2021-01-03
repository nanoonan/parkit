# pylint: disable = invalid-name, broad-except, unused-import
import logging
import os
import platform
import sys
import uuid

import daemoniker

import parkit.constants as constants
import parkit.syslog

from parkit.pool.commands import (
    create_pid_filepath,
    launch_node,
    scan_nodes
)
from parkit.exceptions import log
from parkit.utility import (
    create_string_digest,
    polling_loop
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    pid_filepath = None
    node_uid = None
    cluster_uid = None
    monitor_polling_interval = None
    tasker_polling_interval = None
    cluster_size = None

    try:

        with daemoniker.Daemonizer() as (is_setup, daemonizer):

            if is_setup:
                node_uid = sys.argv[1]
                cluster_uid = sys.argv[2]
                monitor_polling_interval = float(sys.argv[3])
                tasker_polling_interval = float(sys.argv[4])
                cluster_size = int(sys.argv[5])
                pid_filepath = create_pid_filepath(node_uid, cluster_uid)

            is_parent, node_uid, cluster_uid, pid_filepath, monitor_polling_interval, \
            tasker_polling_interval, cluster_size = \
            daemonizer(
                pid_filepath, node_uid, cluster_uid, pid_filepath, monitor_polling_interval, \
                tasker_polling_interval, cluster_size
            )

            if is_parent:
                pass

        if platform.system() == 'Windows':
            del os.environ['__INVOKE_DAEMON__']

        for _ in polling_loop(
            monitor_polling_interval if monitor_polling_interval is not None else \
            constants.DEFAULT_MONITOR_POLLING_INTERVAL
        ):
            running = scan_nodes(cluster_uid if cluster_uid else 'default')
            taskers = [node_uid for node_uid, _ in running if node_uid != 'monitor']
            cluster_size = cluster_size if cluster_size else 0
            if len(taskers) < cluster_size:
                for i in range(cluster_size - len(taskers)):
                    launch_node(
                        create_string_digest(str(uuid.uuid4())),
                        'parkit.pool.taskdaemon',
                        cluster_uid if cluster_uid else 'default',
                        tasker_polling_interval
                    )

    except Exception as exc:
        log(exc)
