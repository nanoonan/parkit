#
# reviewed: 6/16/21
#
import functools
import logging
import os
import threading

from typing import (
    Dict, Optional, Tuple
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.exceptions import SiteNotFoundError
from parkit.storage.environment import get_environment_threadsafe

logger = logging.getLogger(__name__)

site_map: Dict[str, str] = {}

site_map_lock: threading.Lock = threading.Lock()

def set_default_site(
    path: str,
    /, *,
    create: bool = False,
    overwrite_thread_local: bool = True
):
    storage_path = os.path.abspath(path)
    site_uuid = get_site_uuid(storage_path, create = create)
    if overwrite_thread_local or thread.local.default_site is None:
        thread.local.default_site = (storage_path, site_uuid)

def get_default_site() -> Optional[Tuple[str, str]]:
    return thread.local.default_site

def import_site(
    path: str,
    /, *,
    create: bool = False
):
    set_default_site(path, create = create, overwrite_thread_local = False)

@functools.lru_cache(None)
def get_storage_path(site_uuid: str) -> str:
    if site_uuid in site_map:
        return site_map[site_uuid]
    raise SiteNotFoundError()

def get_site_uuid(
    path: str,
    /, *,
    create: bool = False
) -> str:
    storage_path = os.path.abspath(path)
    if storage_path not in site_map:
        with site_map_lock:
            if storage_path not in site_map:
                site_uuid, _, _, _, _, _ = get_environment_threadsafe(
                    storage_path, constants.ROOT_NAMESPACE, create = create
                )
                site_map[site_uuid] = storage_path
                site_map[storage_path] = site_uuid
    return site_map[storage_path]
