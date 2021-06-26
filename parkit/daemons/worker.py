# pylint: disable = broad-except, invalid-name, protected-access
import logging
import os
import queue
import pickle
import sys
import time

import parkit.constants as constants

from parkit.adapters.queue import Queue
from parkit.storage.context import transaction_context
from parkit.storage.environment import get_environment_threadsafe
from parkit.storage.site import get_default_site
from parkit.utility import (
    getenv,
    polling_loop,
    setenv
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    try:

        node_uid = getenv(constants.NODE_UID_ENVNAME, str)
        cluster_uid = getenv(constants.CLUSTER_UID_ENVNAME, str)

        logger.info('worker (%s) started for site %s', node_uid, get_default_site())

        submit_queue = Queue(constants.SUBMIT_QUEUE_PATH, create = True)

        _, environment, _, _, _, _ = get_environment_threadsafe(
            submit_queue.storage_path, submit_queue.namespace,
            create = False
        )

        termination_queue = Queue(constants.NODE_TERMINATION_QUEUE_PATH, create = True)

        polling_interval = getenv(constants.WORKER_POLLING_INTERVAL_ENVNAME, float)

        for i in polling_loop(polling_interval):
            while True:
                try:
                    if len(termination_queue):
                        _ = termination_queue.get()
                        logger.info('worker (%s) terminating on request', node_uid)
                        sys.exit(0)
                except queue.Empty:
                    pass
                try:
                    if len(submit_queue):
                        with transaction_context(environment, write = True):
                            task = submit_queue.get()
                            if task._status == 'submitted':
                                task._status = 'running'
                                task._pid = os.getpid()
                                task._node_uid = node_uid
                                task._start_timestamp = time.time_ns()
                            else:
                                continue
                    else:
                        break
                except queue.Empty:
                    break
                try:
                    result = error = None
                    setenv(
                        constants.SELF_ENVNAME,
                        pickle.dumps(task, 0).decode()
                    )
                    logger.info('start task %s on pid %i', task.asyncable.path, os.getpid())
                    result = task.asyncable.invoke(
                        args = task.args, kwargs = task.kwargs
                    )
                except Exception as exc:
                    logger.exception('error for task: %s', task.asyncable.path)
                    error = exc
                finally:
                    setenv(
                        constants.SELF_ENVNAME,
                        None
                    )
                    with transaction_context(environment, write = True):
                        task._result = result
                        task._error = error
                        task._status = \
                        ('failed' if error is not None else 'finished') \
                        if task._status != 'cancelled' else 'cancelled'
                        task._end_timestamp = time.time_ns()

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(worker) fatal error on pid %i', os.getpid())
