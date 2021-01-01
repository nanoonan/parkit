# pylint: disable = invalid-name, broad-except, protected-access, unused-import
import logging
import os
import platform
import sys

import daemoniker

import parkit.constants as constants
import parkit.logging

from parkit.adapters import Queue
from parkit.pool.commands import create_pid_filepath
from parkit.exceptions import (
    log,
    TransactionError
)
from parkit.storage import transaction
from parkit.utility import polling_loop

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    try:

        pid_filepath = None
        node_uid = None
        cluster_uid = None
        polling_interval = None

        with daemoniker.Daemonizer() as (is_setup, daemonizer):

            if is_setup:
                node_uid = sys.argv[1]
                cluster_uid = sys.argv[2]
                polling_interval = float(sys.argv[3])
                pid_filepath = create_pid_filepath(node_uid, cluster_uid)

            is_parent, node_uid, cluster_uid, pid_filepath, polling_interval = daemonizer(
                pid_filepath, node_uid, cluster_uid, pid_filepath, polling_interval
            )

            if is_parent:
                pass

        if platform.system() == 'Windows':
            del os.environ['__INVOKE_DAEMON__']

        queue = Queue(constants.PROCESS_QUEUE_PATH)

        for _ in polling_loop(
            polling_interval if polling_interval is not None else \
            constants.DEFAULT_TASKER_POLLING_INTERVAL
        ):
            while True:
                process = queue.get()
                if not process:
                    break
                with transaction(process):
                    if process.exists:
                        process._putattr('status', 'running')
                        process._putattr('node_uid', node_uid)
                        process._putattr('pid', os.getpid())
                    else:
                        break
                try:
                    result = exc_value = None
                    target = process._getattr('target')
                    args = process._getattr('args')
                    kwargs = process._getattr('kwargs')
                    if target:
                        result = target(*args, **kwargs)
                    else:
                        result = None
                except Exception as exc:
                    logger.exception('Task daemon caught user error')
                    exc_value = exc
                finally:
                    try:
                        with transaction(process):
                            if process.exists:
                                if exc_value:
                                    process._putattr('status', 'failed')
                                    process._putattr('error', exc_value)
                                else:
                                    process._putattr('status', 'finished')
                        if not exc_value:
                            process._putattr('result', result)
                    except TransactionError:
                        pass

    except Exception as exc:
        log(exc)
