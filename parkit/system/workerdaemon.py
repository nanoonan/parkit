# pylint: disable = broad-except, invalid-name, protected-access
import logging
import os
import queue
import sys
import time

import parkit.constants as constants

from parkit.adapters.dict import Dict
from parkit.adapters.queue import Queue
from parkit.storage.context import transaction_context
from parkit.storage.environment import get_environment_threadsafe
from parkit.storage.site import get_default_site
from parkit.utility import (
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    try:

        node_uid = getenv(constants.NODE_UID_ENVNAME, str)
        cluster_uid = getenv(constants.CLUSTER_UID_ENVNAME, str)

        logger.info('worker (%s) started for site %s', node_uid, get_default_site())

        submit_queue = Queue(constants.SUBMIT_QUEUE_PATH)

        _, environment, _, _, _, _ = get_environment_threadsafe(
            submit_queue.storage_path, submit_queue.namespace,
            create = False
        )

        running_dict = Dict(constants.RUNNING_DICT_PATH)

        termination_queue = Queue(constants.NODE_TERMINATION_QUEUE_PATH)

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
                            execution = submit_queue.get()
                            if execution._status == 'submitted':
                                execution._status = 'running'
                                execution._pid = os.getpid()
                                execution._node_uid = node_uid
                                running_dict[execution.name] = execution
                            else:
                                continue
                    else:
                        break
                except queue.Empty:
                    break
                try:
                    result = error = None
                    result = execution.task.invoke(
                        args = execution.args, kwargs = execution.kwargs
                    )
                except Exception as exc:
                    error = exc
                finally:
                    with transaction_context(environment, write = True):
                        execution._result = result
                        execution._error = error
                        execution._status = \
                        ('failed' if error is not None else 'finished') \
                        if execution._status != 'cancelled' else 'cancelled'
                        execution._end_timestamp = time.time_ns()
                        del running_dict[execution.name]

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(worker) fatal error on pid %i', os.getpid())
