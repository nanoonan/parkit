import logging

from parkit.system.pidtable import set_pid_entry

logger = logging.getLogger(__name__)

set_pid_entry()

logger.info('parkit ready')
