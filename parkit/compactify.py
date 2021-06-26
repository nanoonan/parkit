import logging

from typing import Optional

import parkit.constants as constants

from parkit.adapters.task import Task
from parkit.directory import directories
from parkit.exceptions import StoragePathError

logger = logging.getLogger(__name__)

def compactify(site_uuid: Optional[str] = None):
    for directory in directories(include_hidden = True, site_uuid = site_uuid):
        try:
            if directory.path not in [constants.TASK_NAMESPACE]:
                for obj in directory:
                    if obj.name.startswith('__') and obj.name.endswith('__'):
                        obj.drop()
            elif directory.path in [constants.TASK_NAMESPACE]:
                for obj in directory:
                    if isinstance(obj, Task):
                        if obj.status in ['cancelled', 'finished', 'crashed', 'failed']:
                            obj.drop()
        except StoragePathError:
            pass
