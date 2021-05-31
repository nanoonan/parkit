import logging

from parkit.adapters.task import task

logger = logging.getLogger(__name__)

@task(name = 'test2s')
def ping() -> str:
    logger.info('ping me later')
    return 'ping'
