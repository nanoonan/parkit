# pylint: disable = invalid-name, broad-except
import logging
import os
import time

import daemoniker

import parkit.constants as constants

from parkit.adapters.dict import Dict
from parkit.adapters.scheduler import Scheduler
from parkit.cluster import (
    create_pid_filepath,
    terminate_node
)
from parkit.storage.site import (
    get_site_uuid,
    import_site
)
from parkit.system.functions import directory
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

        for _ in polling_loop(getenv(constants.SCHEDULER_HEARTBEAT_INTERVAL_ENVNAME, float)):
            for scheduler in directory(constants.SCHEDULER_NAMESPACE):
                if isinstance(scheduler, Scheduler):
                    for task, args, kwargs in scheduler.scheduled:
                        task.submit(args = args, kwargs = kwargs)

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(scheduler) fatal error on pid %i', os.getpid())
