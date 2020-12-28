# pylint: disable = W0703, W0212, C0103
import logging
import os
import platform
import sys

import daemoniker
import parkit.constants as constants

from parkit.adapters import Queue
from parkit.pool.commands import create_pid_filepath
from parkit.exceptions import (
    log,
    TransactionError
)
from parkit.storage import transaction
from parkit.utility import polling_loop

# FIXME

logger = logging.getLogger(__name__)

logging.basicConfig(
    format = '[%(asctime)s] %(name)s : %(message)s',
    level = logging.ERROR,
    handlers = [
        logging.FileHandler(
            os.path.join('C:\\users\\rdpuser\\Desktop\\logs', 'python.log')
        )
    ]
)

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

        # FIXME: error handling needs work

        queue = Queue(constants.PROCESS_QUEUE_PATH)
        for _ in polling_loop(polling_interval):
            while True:
                process = queue.get()
                if not process:
                    break
                with transaction(process):
                    if process.exists:
                        process._put('status', 'running')
                        process._put('node_uid', node_uid)
                        process._put('pid', os.getpid())
                    else:
                        break
                try:
                    result = exc_value = None
                    target = process._get('target')
                    args = process._get('args')
                    kwargs = process._get('kwargs')
                    if target:
                        result = target(*args, **kwargs)
                    else:
                        result = None
                except BaseException as exc:
                    logger.exception('taskdaemon caught user error')
                    exc_value = exc
                finally:
                    try:
                        with transaction(process):
                            if process.exists:
                                if exc_value:
                                    process._put('status', 'failed')
                                    process._put('error', exc_value)
                                else:
                                    process._put('status', 'finished')
                        if not exc_value:
                            process._put('result', result)
                    except TransactionError:
                        pass

    except Exception as exc:
        log(exc)
