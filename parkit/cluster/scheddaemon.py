# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import time

from typing import (
    Dict, Optional, Tuple
)

import daemoniker

import parkit.constants as constants

from parkit.adapters import (
    Scheduler,
    Task,
)
from parkit.cluster.manage import create_pid_filepath
from parkit.exceptions import ObjectNotFoundError
from parkit.storage import (
    get_storage_path,
    Namespace
)
from parkit.utility import (
    create_string_digest,
    polling_loop
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':

    pid_filepath = None
    node_uid = None
    cluster_uid = None

    try:

        with daemoniker.Daemonizer() as (is_setup, daemonizer):

            if is_setup:
                assert constants.NODE_UID_ENVNAME in os.environ and \
                constants.CLUSTER_UID_ENVNAME in os.environ
                node_uid = os.environ[constants.NODE_UID_ENVNAME]
                cluster_uid = os.environ[constants.CLUSTER_UID_ENVNAME]
                pid_filepath = create_pid_filepath(node_uid, cluster_uid)

            is_parent, node_uid, cluster_uid, pid_filepath = \
            daemonizer(
                pid_filepath, node_uid, cluster_uid, pid_filepath
            )

            if is_parent:
                pass

        assert cluster_uid == create_string_digest(get_storage_path())

        tasks: Dict[str, Tuple[Task, Optional[int], Optional[Scheduler]]] = {}

        for _ in polling_loop(1):
            seen = set()
            namespace = Namespace(constants.TASK_NAMESPACE)
            for name, descriptor in namespace.descriptors():
                task = None
                try:
                    if descriptor['type'] == 'parkit.adapters.task.Task':
                        if descriptor['uuid'] not in tasks:
                            try:
                                task = namespace[name]
                                if task.scheduler:
                                    tasks[task.uuid] = (task, 0, task.scheduler)
                                else:
                                    tasks[task.uuid] = (task, None, None)
                                seen.add(task.uuid)
                            except KeyError:
                                continue
                        else:
                            seen.add(descriptor['uuid'])
                except ObjectNotFoundError:
                    if task:
                        tasks[task.uuid] = (task, None, None)
                    continue
            for uuid in set(tasks.keys()).difference(seen):
                del tasks[uuid]
            for task, pause_until, scheduler in tasks.values():
                if task.scheduler != scheduler:
                    scheduler = task.scheduler
                    pause_until = 0 if task.scheduler else None
                    tasks[task.uuid] = (task, pause_until, scheduler)
                if pause_until is None:
                    continue
                try:
                    if not task.scheduled:
                        continue
                    now_ns = time.time_ns()
                    if now_ns < pause_until:
                        continue
                    scheduler = task.scheduler
                    if scheduler:
                        try_run, pause_until = scheduler.check_schedule(now_ns)
                        tasks[task.uuid] = (task, pause_until, scheduler)
                        if try_run:
                            task.submit()
                except ObjectNotFoundError:
                    tasks[task.uuid] = (task, None, None)
                    continue

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(scheduler) fatal error on pid %i', os.getpid())
