# pylint: disable = broad-except
import logging
import os

import parkit.constants as constants

from parkit.adapters.scheduler import Scheduler
from parkit.exceptions import ObjectNotFoundError
from parkit.storage.namespace import Namespace
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

        logger.info('scheduler (%s) started for site %s', node_uid, get_default_site())

        for i in polling_loop(getenv(constants.SCHEDULER_HEARTBEAT_INTERVAL_ENVNAME, float)):
            for scheduler in Namespace(constants.SCHEDULER_NAMESPACE, create = True):
                if isinstance(scheduler, Scheduler):
                    try:
                        if scheduler.is_scheduled():
                            scheduler.asyncable(*scheduler.args, **scheduler.kwargs)
                    except ObjectNotFoundError:
                        pass
                    except Exception:
                        logger.exception('error scheduling asyncable')

    except (SystemExit, KeyboardInterrupt, GeneratorExit):
        pass
    except Exception:
        logger.exception('(scheduler) fatal error on pid %i', os.getpid())
