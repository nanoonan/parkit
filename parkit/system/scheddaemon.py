# pylint: disable = invalid-name, broad-except
import logging
import os

import parkit.constants as constants

from parkit.adapters.scheduler import Scheduler
from parkit.storage.site import (
    get_site_uuid,
    import_site
)
from parkit.system.functions import directory
from parkit.utility import (
    getenv,
    polling_loop
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

        for _ in polling_loop(getenv(constants.SCHEDULER_HEARTBEAT_INTERVAL_ENVNAME, float)):
            for scheduler in directory(constants.SCHEDULER_NAMESPACE):
                if isinstance(scheduler, Scheduler):
                    if scheduler.is_scheduled():
                        try:
                            scheduler.task(*scheduler.args, **scheduler.kwargs)
                        except Exception:
                            logger.exception('error scheduling task')

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(scheduler) fatal error on pid %i', os.getpid())
