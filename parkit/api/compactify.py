import logging

import parkit.constants as constants

from parkit.adapters.asyncexecution import AsyncExecution
from parkit.api.directory import directories
from parkit.exceptions import StoragePathError

logger = logging.getLogger(__name__)

def compactify():
    for directory in directories(include_hidden = True):
        try:
            if directory.path not in [constants.EXECUTION_NAMESPACE]:
                for obj in directory:
                    if obj.name.startswith('__') and obj.name.endswith('__'):
                        obj.drop()
            elif directory.path in [constants.EXECUTION_NAMESPACE]:
                for obj in directory:
                    if isinstance(obj, AsyncExecution):
                        if obj.status in ['finished', 'crashed', 'failed']:
                            obj.drop()
        except StoragePathError:
            pass
