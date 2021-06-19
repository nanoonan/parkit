#
# reviewed:
#
import logging
import os

from typing import (
    cast, Iterator, Optional
)

import parkit.storage.threadlocal as thread

from parkit.adapters.object import Object
from parkit.exceptions import SiteNotSpecifiedError
from parkit.storage.namespace import Namespace
from parkit.storage.site import get_storage_path

logger = logging.getLogger(__name__)

class Directory(Namespace):

    def objects(
        self,
        /, *,
        include_hidden: Optional[bool] = None
    ) -> Iterator[Object]:
        return cast(Iterator[Object], self.entities(include_hidden = include_hidden))

def directories(
    path: Optional[str] = None,
    /, *,
    site_uuid: Optional[str] = None,
    include_hidden: bool = False
) -> Iterator[Directory]:
    if site_uuid is not None:
        storage_path = get_storage_path(site_uuid)
    else:
        if thread.local.default_site is not None:
            storage_path, _ = thread.local.default_site
        else:
            raise SiteNotSpecifiedError()
    if path is not None:
        storage_path = os.path.join(storage_path, path)
    for folder_path, _, _ in os.walk(storage_path):
        if folder_path != storage_path:
            namespace = \
            folder_path[len(storage_path):].split(os.path.sep)[1]
            if include_hidden or not (
                namespace.startswith('__') and namespace.endswith('__')
            ):
                if os.path.isfile(os.path.join(folder_path, 'data.mdb')) and \
                os.path.isfile(os.path.join(folder_path, 'lock.mdb')):
                    yield Directory(
                        '/'.join(
                            folder_path[len(storage_path):].split(os.path.sep)[1:]
                        ),
                        include_hidden = include_hidden,
                        site_uuid = site_uuid
                    )

def directory(
    path: Optional[str] = None,
    /, *,
    create: bool = False,
    site_uuid: Optional[str] = None,
    include_hidden: bool = False
) -> Namespace:
    return Directory(
        path, create = create,
        site_uuid = site_uuid,
        include_hidden = include_hidden
    )
