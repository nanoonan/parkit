# pylint: disable = invalid-name
import logging
import os
import sys
import time

from typing import (
    List, Optional, Tuple, Union
)

import lmdb

import parkit.constants as constants
import parkit.storage.transaction as transaction
import parkit.storage.threadlocal as thread

from parkit.adapters.object import Object
from parkit.adapters.scopetable import (
    check_scope,
    dump_scope_table
)
from parkit.pidtable import get_pidtable_snapshot
from parkit.storage.namespace import (
    Namespace,
    namespaces
)
from parkit.utility import (
    get_calling_modules,
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

def directories(site: Optional[str] = None) -> List[str]:
    return sorted([namespace.path for namespace in namespaces(site = site)])

def directory(
    path: Optional[str] = None,
    /, *,
    site: Optional[str] = None
) -> Namespace:
    return Namespace(path, site = site)

def scope_table():
    assert thread.local.storage_path
    return dump_scope_table(thread.local.storage_path.site_uuid)

def pid_table():
    return get_pidtable_snapshot()

def transaction_table() -> List[Tuple[lmdb.Environment, List[lmdb.Transaction]]]:
    table = []
    for env, stack in thread.local.context._stacks.items():
        table.append((env, [context.transaction for context in stack]))
    return table

def gc(site: Optional[str] = None):
    logger.info('garbage collector started on pid %i', os.getpid())
    active_scopes = {uuid for _, (_, uuid) in get_pidtable_snapshot()}
    namespace = Namespace(constants.DEFAULT_NAMESPACE, site = site)
    orphans = []
    for name, descriptor in namespace.descriptors(include_hidden = True):
        if descriptor['anonymous']:
            if descriptor['origin'] not in active_scopes:
                if not check_scope(
                    descriptor['uuid'],
                    namespace.site_uuid,
                    active_scopes
                ):
                    orphans.append(name)
    for name in orphans:
        time.sleep(0)
        try:
            obj = namespace[name]
            obj.drop()
            logger.info('garbage collected %s', name)
        except KeyError:
            continue

def bind_object_to_symbol(obj: Object, overwrite: bool = False) -> bool:
    caller = get_calling_modules()[3]
    if caller == 'IPython.core.interactiveshell':
        module = sys.modules['__main__']
    else:
        module = sys.modules[caller]
    if not overwrite and hasattr(module, obj.name):
        return False
    module.__setattr__(obj.name, obj)
    return True

def bind_symbol(
    namespace: Union[str, Namespace],
    name: str,
    /, *,
    overwrite: bool = False,
    site: Optional[str] = None
) -> bool:
    if isinstance(namespace, str):
        namespace = Namespace(namespace, site = site)
    if name in namespace:
        try:
            return bind_object_to_symbol(namespace[name], overwrite = overwrite)
        except KeyError:
            pass
    return False

def bind_symbols(
    namespace: Union[str, Namespace],
    /, *,
    overwrite: bool = False,
    site: Optional[str] = None
) -> List[str]:
    if isinstance(namespace, str):
        namespace = Namespace(namespace, site = site)
    symbols = []
    for obj in namespace:
        if bind_object_to_symbol(obj, overwrite = overwrite):
            symbols.append(obj.name)
    return symbols

def wait_until(
    condition,
    /, *,
    interval: Optional[float] = None,
    namespace: Optional[Union[Namespace, str]] = None,
    snapshot: Optional[bool] = None,
    timeout: Optional[float] = None
):
    interval = getenv(constants.ADAPTER_POLLING_INTERVAL_ENVNAME, float) \
    if interval is None else interval
    for _ in wait_for(
        condition, interval,
        namespace = namespace, snapshot = snapshot,
        timeout = timeout
    ):
        pass

def wait_for(
    condition,
    interval: float,
    /, *,
    namespace: Optional[Union[Namespace, str]] = None,
    snapshot: Optional[bool] = None,
    timeout: Optional[float] = None
):
    for i in polling_loop(interval, timeout = timeout):
        if snapshot is not None:
            if snapshot:
                with transaction.snapshot(namespace):
                    if condition():
                        return
            else:
                with transaction.transaction(namespace):
                    if condition():
                        return
        else:
            if condition():
                return
        yield i
