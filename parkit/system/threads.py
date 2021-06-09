import logging
import threading

import parkit.constants as constants

from parkit.storage.site import get_sites
from parkit.system.pool import Pool
from parkit.utility import (
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

def monitor_restarter_run():
    for _ in polling_loop(getenv(constants.RESTARTER_POLLING_INTERVAL_ENVNAME, float)):
        for name in get_sites():
            pool = Pool(site = name)
            if not pool.started:
                pool.start()
                logger.info('starting pool for site %s', name)
            elif 'monitor' not in [node_uid.split('-')[0] for node_uid in pool.nodes]:
                pool.start()
                logger.info('restarting pool for site %s', name)

monitor_restarter = threading.Thread(target = monitor_restarter_run, daemon = True)
