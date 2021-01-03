# pylint: disable = invalid-name, broad-except, protected-access, unused-import
import logging
import os
import platform
import queue
import sys

import daemoniker

import parkit.constants as constants
import parkit.syslog

from parkit.adapters import ProcessQueue
from parkit.pool.commands import create_pid_filepath
from parkit.exceptions import (
    log,
    ObjectNotFoundError,
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

        process_queue = ProcessQueue(constants.PROCESS_QUEUE_PATH)

        for _ in polling_loop(
            polling_interval if polling_interval is not None else \
            constants.DEFAULT_TASKER_POLLING_INTERVAL
        ):
            while True:
                with transaction(constants.PROCESS_NAMESPACE):
                    try:
                        process = process_queue.get_nowait()
                    except ObjectNotFoundError:
                        continue
                    except queue.Empty:
                        break
                    process._Process__put('status', 'running')
                    process._Process__put('node_uid', node_uid)
                    process._Process__put('pid', os.getpid())
                try:
                    result = exc_value = None
                    result = process.run(process)
                except Exception as exc:
                    logger.exception('Task daemon caught user error')
                    exc_value = exc
                finally:
                    try:
                        with transaction(process):
                            if exc_value:
                                process._Process__put('status', 'failed')
                                process._Process__put('error', exc_value)
                            else:
                                process._Process__put('status', 'finished')
                                process._Process__put('result', result)
                    except ObjectNotFoundError:
                        pass

    except Exception as exc:
        log(exc)
