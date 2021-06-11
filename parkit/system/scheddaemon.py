# pylint: disable = invalid-name, broad-except, protected-access
import logging
import os
import time
import typing

from typing import (
    cast, Optional, Tuple
)

import daemoniker

import parkit.constants as constants

from parkit.adapters.dict import Dict
from parkit.adapters.scheduler import Scheduler
from parkit.adapters.task import Task
from parkit.cluster import (
    create_pid_filepath,
    terminate_node
)
from parkit.exceptions import ObjectNotFoundError
from parkit.storage.namespace import Namespace
from parkit.storage.site import (
    get_site_uuid,
    import_site
)
from parkit.system.pool import acquire_daemon_lock
from parkit.utility import (
    getenv,
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

        storage_path = getenv(constants.CLUSTER_STORAGE_PATH_ENVNAME, str)
        site_uuid = getenv(constants.CLUSTER_SITE_UUID_ENVNAME, str)
        import_site(storage_path, name = 'main')
        assert get_site_uuid('main') == site_uuid

        pool_state = Dict(constants.POOL_STATE_DICT_PATH)

        if not acquire_daemon_lock('scheduler', pool_state):
            logger.info('scheduler failed to acquire lock...exiting (%s)', node_uid)
            assert node_uid is not None and cluster_uid is not None
            terminate_node(node_uid, cluster_uid)
            while True:
                time.sleep(1)

        tasks: typing.Dict[str, Tuple[Task, typing.Dict[Scheduler, Optional[int]]]] = {}

        for _ in polling_loop(getenv(constants.SCHEDULER_HEARTBEAT_INTERVAL_ENVNAME, float)):
            seen = set()
            namespace = Namespace(constants.TASK_NAMESPACE)
            names = []
            for name, descriptor in namespace.descriptors():
                if descriptor['type'] == 'parkit.adapters.task.Task':
                    if descriptor['uuid'] not in tasks:
                        names.append(name)
                    else:
                        seen.add(descriptor['uuid'])
            for name in names:
                try:
                    task = cast(Task, namespace[name])
                    tasks[task.uuid] = (task, {})
                    seen.add(task.uuid)
                except KeyError:
                    continue
            for uuid in set(tasks.keys()).difference(seen):
                del tasks[uuid]
            schedulers: typing.Dict[Scheduler, Optional[int]]
            for task, schedulers in tasks.values():
                try:
                    latest_schedulers = set(task.schedulers)
                    known_schedulers = set(schedulers.keys())
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
                            request = task.get_args(scheduler)
                            if request is not None:
                                args, kwargs = request
                                task.submit(args = args, kwargs = kwargs)
                        if pause_until is None:
                            task.unschedule(scheduler)
                except ObjectNotFoundError:
                    continue

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(scheduler) fatal error on pid %i', os.getpid())
