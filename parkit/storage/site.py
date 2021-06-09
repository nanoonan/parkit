import logging
import uuid

from typing import (
    Dict, List, Optional
)

import parkit.storage.threadlocal as thread

from parkit.exceptions import SiteNotFoundError
from parkit.storage.threadlocal import StoragePath

logger = logging.getLogger(__name__)

site_map: Dict[str, str] = {}

def get_site_uuid(name: str):
    if name in site_map.values():
        for site_uuid, site_name in site_map.items():
            if name == site_name:
                return site_uuid
    raise SiteNotFoundError()

def get_site_name(site_uuid: str):
    if site_uuid in site_map:
        return site_map[site_uuid]
    raise SiteNotFoundError()

def import_site(
    path: str,
    /, *,
    name: Optional[str] = None
):
    storage_path = StoragePath(path = path)
    for site_uuid, site_name in site_map.copy().items():
        if site_uuid == storage_path.site_uuid:
            if not name:
                return
            if site_name == name:
                return
    if name in site_map.values():
        for site_uuid, site_name in site_map.copy().items():
            if name == site_name:
                del site_map[site_uuid]
                if thread.local.storage_path:
                    if thread.local.storage_path.site_uuid == site_uuid:
                        thread.local.storage_path = None
    if not name:
        name = str(uuid.uuid4())
    site_map[storage_path.site_uuid] = name
    if thread.local.storage_path is None:
        if not (name.startswith('__') and name.endswith('__')):
            thread.local.storage_path = storage_path

def set_site(name: str):
    if name not in site_map.values():
        raise SiteNotFoundError()
    for site_uuid, site_name in site_map.items():
        if name == site_name:
            thread.local.storage_path = StoragePath(site_uuid = site_uuid)

def current_site() -> Optional[str]:
    if thread.local.storage_path:
        assert thread.local.storage_path.site_uuid in site_map
        return site_map[thread.local.storage_path.site_uuid]
    return None

def get_sites(*, include_hidden: bool = False) -> List[str]:
    if include_hidden:
        return list(site_map.values())
    return [
        name for name in list(site_map.values()) \
        if not (name.startswith('__') and name.endswith('__'))
    ]
