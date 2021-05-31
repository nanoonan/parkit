# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import platform
import queue
import sys
import time

import daemoniker

import parkit.constants as constants

from parkit.adapters import Queue
from parkit.exceptions import ObjectNotFoundError
from parkit.pool.commands import create_pid_filepath
from parkit.storage import transaction
from parkit.utility import (
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

Qself = None

if __name__ == '__main__':

    try:

        pid_filepath = None
        node_uid = None
        cluster_uid = None

        with daemoniker.Daemonizer() as (is_setup, daemonizer):

            if is_setup:
                node_uid = sys.argv[1]
                cluster_uid = sys.argv[2]
                pid_filepath = create_pid_filepath(node_uid, cluster_uid)

            is_parent, node_uid, cluster_uid, pid_filepath = daemonizer(
                pid_filepath, node_uid, cluster_uid, pid_filepath
            )

            if is_parent:
                pass

        if platform.system() == 'Windows':
            del os.environ['__INVOKE_DAEMON__']

        task_queue = Queue(constants.TASK_QUEUE_PATH)

        termination_queue = Queue(constants.NODE_TERMINATION_QUEUE_PATH)

        polling_interval = getenv(constants.WORKER_POLLING_INTERVAL_ENVNAME, float)

        for _ in polling_loop(polling_interval):
            while True:
                try:
                    if len(termination_queue):
                        _ = termination_queue.get_nowait()
                        sys.exit(0)
                except queue.Empty:
                    pass
                try:
                    if len(task_queue):
                        task, trace_index, args, kwargs = \
                        task_queue.get_nowait()
                    else:
                        break
                except ObjectNotFoundError:
                    continue
                except queue.Empty:
                    break
                try:
                    with transaction(constants.TASK_NAMESPACE):
                        record = task._Function__traces[trace_index]
                        if record['status'] == 'submitted':
                            record['pid'] = os.getpid()
                            record['node_uuid'] = node_uid
                            record['status'] = 'running'
                            task._Function__traces[trace_index] = record
                        else:
                            assert record['status'] == 'cancelled'
                            continue
                except ObjectNotFoundError:
                    continue
                try:
                    result = exc_value = None
                    result = task.invoke(
                        args = args, kwargs = kwargs
                    )
                except Exception as exc:
                    logger.exception('(worker) invoke error on pid %i', os.getpid())
                    exc_value = exc
                finally:
                    try:
                        with transaction(constants.TASK_NAMESPACE):
                            record = task._Function__traces[trace_index]
                            record['result'] = result
                            record['error'] = exc_value
                            record['status'] = 'failed' if exc_value else 'finished'
                            record['end_timestamp'] = time.time_ns()
                            task._Function__traces[trace_index] = record
                    except ObjectNotFoundError:
                        pass

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(worker) fatal error on pid %i', os.getpid())
