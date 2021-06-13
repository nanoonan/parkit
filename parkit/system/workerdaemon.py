# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import queue
import sys
import time
import uuid

import parkit.constants as constants

from parkit.adapters.queue import Queue
from parkit.exceptions import ObjectNotFoundError
from parkit.storage.context import transaction_context
from parkit.storage.site import (
    get_site_uuid,
    import_site
)
from parkit.system.pidtable import set_pid_entry
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
        storage_path = getenv(constants.CLUSTER_STORAGE_PATH_ENVNAME, str)
        site_uuid = getenv(constants.CLUSTER_SITE_UUID_ENVNAME, str)
        import_site(storage_path, name = 'main')
        assert get_site_uuid('main') == site_uuid

        execution_queue = Queue(constants.EXECUTION_QUEUE_PATH)

        termination_queue = Queue(constants.NODE_TERMINATION_QUEUE_PATH)

        polling_interval = getenv(constants.WORKER_POLLING_INTERVAL_ENVNAME, float)

        logger.info('worker ready %s', node_uid)

        for _ in polling_loop(polling_interval):
            while True:
                try:
                    if len(termination_queue):
                        _ = termination_queue.get()
                        logger.info('terminating worker pid = %i uuid = %s', os.getpid(), node_uid)
                        sys.exit(0)
                except queue.Empty:
                    pass
                try:
                    if len(execution_queue):
                        setenv(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME, site_uuid)
                        with transaction_context(execution_queue._Entity__env, write = True):
                            execution = execution_queue.get()
                            if execution._Entity__env == execution_queue._Entity__env and \
                            execution._AsyncExecution__status == 'submitted':
                                execution._AsyncExecution__status = 'running'
                                execution._AsyncExecution__pid = os.getpid()
                                execution._AsyncExecution__node_uid = node_uid
                            else:
                                continue
                    else:
                        break
                except (AttributeError, ObjectNotFoundError):
                    continue
                except queue.Empty:
                    break
                finally:
                    setenv(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME, None)
                try:
                    task = execution._AsyncExecution__task
                    arguments = execution._AsyncExecution__arguments
                    result = error = None
                    result = task.invoke(
                        args = arguments.args, kwargs = arguments.kwargs
                    )
                except Exception as exc:
                    logger.exception('(worker) invoke error on pid %i', os.getpid())
                    error = exc
                finally:
                    try:
                        with transaction_context(execution._Entity__env, write = True):
                            execution._AsyncExecution__result = result
                            execution._AsyncExecution__error = error
                            execution._AsyncExecution__status = 'failed' \
                            if error is not None else 'finished'
                            execution._AsyncExecution__end_timestamp = time.time_ns()
                    except (AttributeError, ObjectNotFoundError):
                        pass
                    finally:
                        setenv(constants.PROCESS_UUID_ENVNAME, str(uuid.uuid4()))
                        set_pid_entry()

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(worker) fatal error on pid %i', os.getpid())
