# pylint: disable = protected-access
import logging

from typing import (
    Any, Optional
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.storage.entity import Entity
from parkit.storage.site import import_site
from parkit.utility import getenv

logger = logging.getLogger(__name__)

import_site(
    getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str),
    name = constants.GLOBAL_SITE_NAME
)

class Synchronized():

    def __init__(self, lock_id: str):
        self._lock_id = lock_id
        self._lock = Entity(
            constants.SYNCHRONIZED_NAMESPACE, ''.join(['__lock_', lock_id, '__']),
            site = constants.GLOBAL_SITE_NAME
        )

    def __enter__(self):
        thread.local.context.push(self._lock._Entity__env, True)
        return self

    def __exit__(self, error_type: type, error: Optional[Any], traceback: Any):
        thread.local.context.pop(self._lock._Entity__env, error)
        if error is not None:
            raise error

def synchronized(lock_id: str):
    return Synchronized(lock_id)
