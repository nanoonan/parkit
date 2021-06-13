import logging

from typing import (
    ContextManager, Optional, Union
)

import parkit.storage.threadlocal as thread

from parkit.storage.context import transaction_context
from parkit.storage.entity import Entity
from parkit.storage.environment import get_environment_threadsafe
from parkit.exceptions import SiteNotSpecifiedError
from parkit.storage.namespace import Namespace
from parkit.storage.site import get_site_uuid
from parkit.storage.threadlocal import StoragePath
from parkit.utility import resolve_namespace

logger = logging.getLogger(__name__)

def transaction(
    obj: Optional[Union[str, Namespace, Entity]] = None,
    /, *,
    zero_copy: bool = True,
    site: Optional[str] = None,
    site_uuid: Optional[str] = None
) -> ContextManager:
    if obj is None or isinstance(obj, str):
        assert site or site_uuid or thread.local.storage_path
        namespace = resolve_namespace(obj)
        if not site_uuid:
            if site:
                site_uuid = get_site_uuid(site)
            elif thread.local.storage_path:
                site_uuid = thread.local.storage_path.site_uuid
            else:
                raise SiteNotSpecifiedError()
        storage_path = StoragePath(site_uuid = site_uuid).path
    elif isinstance(obj, Namespace):
        namespace = obj.path
        storage_path = obj.storage_path
    elif isinstance(obj, Entity):
        namespace = obj.namespace
        storage_path = obj.storage_path
    else:
        raise ValueError()
    _, env, _, _, _, _ = get_environment_threadsafe(storage_path, namespace)
    return transaction_context(
        env, write = True, buffers = zero_copy
    )

def snapshot(
    obj: Optional[Union[str, Namespace, Entity]] = None,
    /, *,
    zero_copy: bool = True,
    site: Optional[str] = None,
    site_uuid: Optional[str] = None
) -> ContextManager:
    if obj is None or isinstance(obj, str):
        assert site or site_uuid or thread.local.storage_path
        namespace = resolve_namespace(obj)
        if not site_uuid:
            if site:
                site_uuid = get_site_uuid(site)
            elif thread.local.storage_path:
                site_uuid = thread.local.storage_path.site_uuid
            else:
                raise SiteNotSpecifiedError()
        storage_path = StoragePath(site_uuid = site_uuid).path
    elif isinstance(obj, Namespace):
        namespace = obj.path
        storage_path = obj.storage_path
    elif isinstance(obj, Entity):
        namespace = obj.namespace
        storage_path = obj.storage_path
    else:
        raise ValueError()
    _, env, _, _, _, _ = get_environment_threadsafe(storage_path, namespace)
    return transaction_context(
        env, write = False, buffers = zero_copy
    )
