#
# reviewed: 6/16/21
#
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
from parkit.storage.site import get_storage_path
from parkit.utility import resolve_namespace

logger = logging.getLogger(__name__)

def transaction(
    obj: Optional[Union[str, Namespace, Entity]] = None,
    /, *,
    site_uuid: Optional[str] = None
) -> ContextManager:
    if obj is None or isinstance(obj, str):
        namespace = resolve_namespace(obj)
        if site_uuid is not None:
            storage_path = get_storage_path(site_uuid)
        else:
            if thread.local.default_site is not None:
                storage_path, _ = thread.local.default_site
            else:
                raise SiteNotSpecifiedError()
    elif isinstance(obj, Namespace):
        namespace = obj.path
        storage_path = obj.storage_path
    elif isinstance(obj, Entity):
        namespace = obj.namespace
        storage_path = obj.storage_path
    else:
        raise ValueError()
    _, env, _, _, _, _ = get_environment_threadsafe(storage_path, namespace, create = False)
    return transaction_context(env, write = True)

def snapshot(
    obj: Optional[Union[str, Namespace, Entity]] = None,
    /, *,
    site_uuid: Optional[str] = None
) -> ContextManager:
    if obj is None or isinstance(obj, str):
        namespace = resolve_namespace(obj)
        if site_uuid is not None:
            storage_path = get_storage_path(site_uuid)
        else:
            if thread.local.default_site is not None:
                storage_path, _ = thread.local.default_site
            else:
                raise SiteNotSpecifiedError()
    elif isinstance(obj, Namespace):
        namespace = obj.path
        storage_path = obj.storage_path
    elif isinstance(obj, Entity):
        namespace = obj.namespace
        storage_path = obj.storage_path
    else:
        raise ValueError()
    _, env, _, _, _, _ = get_environment_threadsafe(storage_path, namespace, create = False)
    return transaction_context(env, write = False)
