import logging
import sys

from typing import (
    List, Optional, Union
)

import parkit.constants as constants
import parkit.storage as storage

from parkit.adapters import Object
from parkit.utility import (
    get_calling_modules,
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

def bind_symbol(obj: Object, overwrite: bool = False) -> bool:
    caller = get_calling_modules()[3]
    if caller == 'IPython.core.interactiveshell':
        module = sys.modules['__main__']
    else:
        module = sys.modules[caller]
    if not overwrite and hasattr(module, obj.name):
        return False
    module.__setattr__(obj.name, obj)
    return True

def bind_symbols(
    namespace: Union[str, storage.Namespace],
    /, *,
    overwrite: bool = False
) -> List[str]:
    if isinstance(namespace, str):
        namespace = storage.Namespace(namespace)
    symbols = []
    for obj in namespace:
        if bind_symbol(obj, overwrite = overwrite):
            symbols.append(obj.name)
    return symbols

def wait_until(
    condition,
    /, *,
    interval: Optional[float] = None,
    namespace: Optional[Union[storage.Namespace, str]] = None,
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
    namespace: Optional[Union[storage.Namespace, str]] = None,
    snapshot: Optional[bool] = None,
    timeout: Optional[float] = None
):
    for i in polling_loop(interval, timeout = timeout):
        if snapshot is not None:
            if snapshot:
                with storage.snapshot(namespace):
                    if condition():
                        return
            else:
                with storage.transaction(namespace):
                    if condition():
                        return
        else:
            if condition():
                return
        yield i
