# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import platform
import queue
import time
import uuid

import daemoniker

import parkit.constants as constants

from parkit.adapters.queue import Queue
from parkit.cluster.manage import (
    create_pid_filepath,
    terminate_node
)
from parkit.exceptions import ObjectNotFoundError
from parkit.functions import bind_symbol
from parkit.pidtable import set_pid_entry
from parkit.storage.transaction import transaction

from parkit.utility import (
    create_string_digest,
    getenv,
    polling_loop,
    setenv
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    try:

        pid_filepath = None
        node_uid = None
        cluster_uid = None

        with daemoniker.Daemonizer() as (is_setup, daemonizer):

            if is_setup:
                assert constants.NODE_UID_ENVNAME in os.environ and \
                constants.CLUSTER_UID_ENVNAME in os.environ
                node_uid = os.environ[constants.NODE_UID_ENVNAME]
                cluster_uid = os.environ[constants.CLUSTER_UID_ENVNAME]
                pid_filepath = create_pid_filepath(node_uid, cluster_uid)

            is_parent, node_uid, cluster_uid, pid_filepath = daemonizer(
                pid_filepath, node_uid, cluster_uid, pid_filepath
            )

            if is_parent:
                pass

        if platform.system() == 'Windows':
            os.environ['__PARKIT_DAEMON__'] = 'True'
            if '__INVOKE_DAEMON__' in os.environ:
                del os.environ['__INVOKE_DAEMON__']

        # storage_path = get_storage_path()
        # assert storage_path is not None
        # assert cluster_uid == create_string_digest(storage_path)
        storage_path = None

        task_queue = Queue(constants.TASK_QUEUE_PATH)

        termination_queue = Queue(constants.NODE_TERMINATION_QUEUE_PATH)

        polling_interval = getenv(constants.WORKER_POLLING_INTERVAL_ENVNAME, float)

        logger.info('worker ready %s', node_uid)
        for _ in polling_loop(polling_interval):
            while True:
                try:
                    if len(termination_queue):
                        _ = termination_queue.get_nowait()
                        logger.info('terminating worker pid = %i uuid = %s', os.getpid(), node_uid)
                        assert node_uid is not None
                        terminate_node(
                            node_uid,
                            storage_path
                        )
                        while True:
                            time.sleep(1)
                except queue.Empty:
                    pass
                try:
                    if len(task_queue):
                        setenv(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME, 'True')
                        with transaction(constants.TASK_NAMESPACE):
                            task, trace_index, args, kwargs = task_queue.get_nowait()
                            record = task._Function__traces[trace_index]
                            if record['status'] == 'submitted':
                                record['pid'] = os.getpid()
                                record['node_uid'] = node_uid
                                record['status'] = 'running'
                                task._Function__traces[trace_index] = record
                            else:
                                assert record['status'] == 'cancelled'
                                continue
                    else:
                        break
                except ObjectNotFoundError:
                    continue
                except queue.Empty:
                    break
                finally:
                    setenv(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME, None)
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
                    finally:
                        setenv(constants.PROCESS_UUID_ENVNAME, str(uuid.uuid4()))
                        set_pid_entry()

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(worker) fatal error on pid %i', os.getpid())
