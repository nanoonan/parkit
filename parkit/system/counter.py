import logging

import parkit.constants as constants

from parkit.adapters.object import Object
from parkit.storage.context import transaction_context
from parkit.storage.site import (
    get_site_uuid,
    import_site
)
from parkit.utility import (
    create_string_digest,
    getenv
)

logger = logging.getLogger(__name__)

import_site(getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str), create = True)

class Counter(Object):

    def __init__(self, counter_id: str):

        self.__counter: int

        def on_init(created: bool):
            if created:
                self.__counter = 0

        super().__init__(
            '/'.join([constants.COUNTER_NAMESPACE, create_string_digest(counter_id)]),
            versioned = False, on_init = on_init,
            site_uuid = get_site_uuid(getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str))
        )

    def get_value(self) -> int:
        with transaction_context(self._env, write = True):
            value = self.__counter
            self.__counter += 1
            return value

def get_counter_value(counter_id: str) -> int:
    return Counter(counter_id).get_value()
