# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import queue
import time
import uuid

import daemoniker

import parkit.constants as constants

from parkit.adapters.queue import Queue
from parkit.cluster import (
    create_pid_filepath,
    terminate_node
)
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

        storage_path = getenv(constants.CLUSTER_STORAGE_PATH_ENVNAME, str)
        site_uuid = getenv(constants.CLUSTER_SITE_UUID_ENVNAME, str)
        import_site(storage_path, name = 'main')
        assert get_site_uuid('main') == site_uuid

        task_queue = Queue(constants.TASK_QUEUE_PATH)

        termination_queue = Queue(constants.NODE_TERMINATION_QUEUE_PATH)

        polling_interval = getenv(constants.WORKER_POLLING_INTERVAL_ENVNAME, float)

        logger.info('worker ready %s', node_uid)
        for _ in polling_loop(polling_interval):
            while True:
                try:
                    if len(termination_queue):
                        _ = termination_queue.get()
                        logger.info('terminating worker pid = %i uuid = %s', os.getpid(), node_uid)
                        assert node_uid is not None and cluster_uid is not None
                        terminate_node(
                            node_uid,
                            cluster_uid
                        )
                        while True:
                            time.sleep(1)
                except queue.Empty:
                    pass
                try:
                    if len(task_queue):
                        setenv(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME, site_uuid)
                        with transaction_context(task_queue._Entity__env, write = True):
                            task, trace_index, args, kwargs = task_queue.get()
                            assert task._Entity__env == task_queue._Entity__env
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
                        with transaction_context(task_queue._Entity__env, write = True):
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
