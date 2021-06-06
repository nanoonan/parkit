# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import time

from typing import (
    Dict, Optional, Tuple
)

import daemoniker

import parkit.constants as constants

from parkit.adapters.scheduler import Scheduler
from parkit.adapaters.task import Task
from parkit.cluster.manage import create_pid_filepath
from parkit.exceptions import ObjectNotFoundError
from parkit.storage.namespace import Namespace
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

        # storage_path = get_storage_path()
        # assert storage_path is not None
        # assert cluster_uid == create_string_digest(storage_path)

        tasks: Dict[str, Tuple[Task, Dict[Scheduler, Optional[int]]]] = {}

        for _ in polling_loop(1):
            seen = set()
            namespace = Namespace(constants.TASK_NAMESPACE)
            for name, descriptor in namespace.descriptors():
                if descriptor['type'] == 'parkit.adapters.task.Task':
                    if descriptor['uuid'] not in tasks:
                        try:
                            task = namespace[name]
                            tasks[task.uuid] = (task, {})
                            seen.add(task.uuid)
                        except KeyError:
                            continue
                    else:
                        seen.add(descriptor['uuid'])
            for uuid in set(tasks.keys()).difference(seen):
                del tasks[uuid]
            schedulers: Dict[Scheduler, Optional[int]]
            for task, schedulers in tasks.values():
                try:
                    latest_schedulers = set(task.schedulers)
                    known_schedulers = {scheduler for scheduler in schedulers.keys()}
                    added = latest_schedulers.difference(known_schedulers)
                    removed = known_schedulers.difference(latest_schedulers)
                    for scheduler in removed:
                        del schedulers[scheduler]
                    for scheduler in added:
                        schedulers[scheduler] = 0
                    for scheduler, pause_until in schedulers.items():
                        if pause_until is None:
                            continue
                        now_ns = time.time_ns()
                        if now_ns < pause_until:
                            continue
                        run, pause_until = scheduler.check_schedule(now_ns)
                        schedulers[scheduler] = pause_until
                        if run:
                            args, kwargs = task.get_args(scheduler)
                            task.submit(args = args, kwargs = kwargs)
                        if pause_until is None:
                            task.unschedule(scheduler)
                except ObjectNotFoundError:
                    continue

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(scheduler) fatal error on pid %i', os.getpid())
