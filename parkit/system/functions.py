# pylint: disable = invalid-name
import logging
import os
import sys
import time

from typing import (
    Any, cast, Dict, Iterator, List, Optional, Tuple, Union
)

from tabulate import tabulate

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.object import Object
from parkit.adapters.scopetable import (
    check_scope,
    dump_scope_table
)
from parkit.exceptions import SiteNotSpecifiedError
from parkit.storage.entity import Entity
from parkit.storage.namespace import Namespace
from parkit.storage.site import (
    get_site_uuid,
    get_sites
)
from parkit.storage.threadlocal import StoragePath
from parkit.system.pidtable import get_pidtable_snapshot
from parkit.system.pool import Pool
from parkit.utility import get_calling_modules

logger = logging.getLogger(__name__)

class Directory(Namespace):

    def objects(self, /, *, include_hidden: bool = False) -> Iterator[Object]:
        return cast(Iterator[Object], self.entities(include_hidden = include_hidden))

def show(target: Any):
    if isinstance(target, Directory):
        rows = []
        for obj in target.objects():
            rows.append([
                obj.path,
                obj.descriptor['type'],
                obj.created
            ])
        if rows:
            rows = sorted(rows, key = lambda x: x[0])
            print(tabulate(
                rows,
                showindex = False,
                headers = ['path', 'type', 'created'],
                tablefmt = 'fancy_grid'
            ))
        else:
            print('no objects in', target.path)

def pool(site: Optional[str] = None) -> Pool:
    return Pool(site = site)

def directories(
    site: Optional[str] = None,
    /, *,
    include_hidden: bool = False
) -> Iterator[Directory]:
    if site:
        site_uuid = get_site_uuid(site)
    elif thread.local.storage_path:
        site_uuid = thread.local.storage_path.site_uuid
    else:
        raise SiteNotSpecifiedError()
    storage_path = StoragePath(site_uuid = site_uuid).path
    for folder, _, _ in os.walk(storage_path):
        if folder != storage_path:
            top_level_namespace = \
            folder[len(storage_path):].split(os.path.sep)[1]
            if include_hidden or not (
                top_level_namespace.startswith('__') and top_level_namespace.endswith('__')
            ):
                yield Directory('/'.join(
                    folder[len(storage_path):].split(os.path.sep)[1:]
                ))

def directory(
    path: Optional[str] = None,
    /, *,
    site: Optional[str] = None
) -> Namespace:
    return Directory(path, site = site)

def scope_table():
    assert thread.local.storage_path
    return dump_scope_table(thread.local.storage_path.site_uuid)

def pid_table() -> Dict[int, Tuple[float, str]]:
    return get_pidtable_snapshot()

def gc(*, site: Optional[str] = None, all_sites: bool = False):

    def garbage_collect(site: Optional[str]):
        logger.info('garbage collector started on pid %i', os.getpid())
        active_scopes = {uuid for _, (_, uuid) in get_pidtable_snapshot().items()}
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
    if all_sites:
        for name in get_sites():
            garbage_collect(name)
    else:
        garbage_collect(site)

def bind_entity_to_symbol(entity: Entity, overwrite: bool = False) -> bool:
    caller = get_calling_modules()[3]
    if caller == 'IPython.core.interactiveshell':
        module = sys.modules['__main__']
    else:
        module = sys.modules[caller]
    if not overwrite and hasattr(module, entity.name):
        return False
    module.__setattr__(entity.name, entity)
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
            return bind_entity_to_symbol(namespace[name], overwrite = overwrite)
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
    for entity in namespace:
        if bind_entity_to_symbol(entity, overwrite = overwrite):
            symbols.append(entity.name)
    return symbols
