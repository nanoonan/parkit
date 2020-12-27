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
    ObjectNotFoundError
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

        queue = Queue(constants.PROCESS_QUEUE_PATH)
        for _ in polling_loop(polling_interval):
            while True:
                process = queue.get()
                if not process:
                    break
                with transaction(process):
                    if process.exists:
                        process._put_attribute('_status', 'running')
                        process._put_attribute('_node_uid', node_uid)
                        process._put_attribute('_pid', os.getpid())
                        target = process._get_attribute('_target')
                        args = process._get_attribute('_args')
                        kwargs = process._get_attribute('_kwargs')
                    else:
                        break
                try:
                    result = exc_value = None
                    if target:
                        result = target(*args, **kwargs)
                    else:
                        result = None
                except BaseException as e:
                    exc_value = e
                finally:
                    with transaction(process):
                        try:
                            if process.exists:
                                if exc_value is None:
                                    process._put_attribute('_status', 'finished')
                                    process._put_attribute('_result', result)
                                else:
                                    process._put_attribute('_status', 'failed')
                                    process._put_attribute('_error', exc_value)
                        except ObjectNotFoundError:
                            pass

    except Exception as e:
        log(e)
