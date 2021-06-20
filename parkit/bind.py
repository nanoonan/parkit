import logging
import sys

from typing import (
    List, Optional, Union
)

from parkit.directory import directories
from parkit.exceptions import ObjectNotFoundError
from parkit.storage.entity import Entity
from parkit.storage.namespace import Namespace
from parkit.utility import get_calling_modules

logger = logging.getLogger(__name__)

def bind_entity_to_symbol(entity: Entity, overwrite: bool = False) -> bool:
    caller = get_calling_modules()[4]
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
    site_uuid: Optional[str] = None
) -> bool:
    if isinstance(namespace, str):
        namespace = Namespace(namespace, site_uuid = site_uuid)
    try:
        return bind_entity_to_symbol(namespace[name], overwrite = overwrite)
    except ObjectNotFoundError:
        pass
    return False

def bind_symbols(
    namespace: Union[str, Namespace],
    /, *,
    overwrite: bool = False,
    site_uuid: Optional[str] = None,
    recursive: bool = False
) -> List[str]:
    if isinstance(namespace, str):
        namespace = Namespace(namespace, site_uuid = site_uuid)
    symbols = []

    def bind(namespace: Namespace):
        for entity in namespace:
            if bind_entity_to_symbol(entity, overwrite = overwrite):
                symbols.append(entity.name)

    bind(namespace)
    if recursive:
        for descendant in directories(namespace.path, site_uuid = site_uuid):
            bind(descendant)
    return symbols
