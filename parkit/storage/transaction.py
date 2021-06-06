import logging

from typing import (
    Any, ContextManager, Optional, Union
)

import parkit.storage.threadlocal as thread

from parkit.storage.context import transaction_context
from parkit.storage.environment import get_environment_threadsafe
from parkit.exceptions import SiteNotSpecifiedError
from parkit.storage.namespace import Namespace
from parkit.storage.site import get_site_uuid
from parkit.storage.threadlocal import StoragePath
from parkit.utility import resolve_namespace

logger = logging.getLogger(__name__)

def transaction(
    obj: Optional[Union[str, Any]] = None,
    /, *,
    zero_copy: bool = True,
    site: Optional[str] = None
) -> ContextManager:
    if obj is None or isinstance(obj, str):
        assert site or thread.local.storage_path
        namespace = resolve_namespace(obj)
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
    else:
        raise ValueError()
    _, env, _, _, _, _ = get_environment_threadsafe(storage_path, namespace)
    return transaction_context(env, write = True, inherit = False, buffers = zero_copy)

def snapshot(
    obj: Optional[Union[str, Any]] = None,
    /, *,
    zero_copy: bool = True,
    site: Optional[str] = None
) -> ContextManager:
    if obj is None or isinstance(obj, str):
        assert site or thread.local.storage_path
        namespace = resolve_namespace(obj)
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
    else:
        raise ValueError()
    _, env, _, _, _, _ = get_environment_threadsafe(storage_path, namespace)
    return transaction_context(env, write = False, inherit = True, buffers = zero_copy)
