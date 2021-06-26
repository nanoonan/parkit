# pylint: disable = protected-access
import logging

from typing import (
    Any, Iterator
)

from parkit.adapters.array import Array
from parkit.storage.context import transaction_context
from parkit.storage.wait import wait

logger = logging.getLogger(__name__)

def stream(
    source: Array,
    /, *,
    batch: bool = False
) -> Iterator[Any]:

    with transaction_context(source._env, write = False):
        version = source.version
        index = len(source)

    while True:
        wait(source, lambda: source.version > version)
        with transaction_context(source._env, write = False):
            cache = []
            n_new_entries = source.version - version
            if source.maxsize is None:
                for _ in range(n_new_entries):
                    if batch:
                        cache.append(source[index])
                    else:
                        yield source[index]
                    index += 1
                if batch and cache:
                    yield cache
            else:
                while index < source.maxsize:
                    if n_new_entries == 0:
                        break
                    if batch:
                        cache.append(source[index])
                    else:
                        yield source[index]
                    n_new_entries -= 1
                    index += 1
                for i in range(source.maxsize - n_new_entries, source.maxsize):
                    if batch:
                        cache.append(source[i])
                    else:
                        yield source[i]
                if batch and cache:
                    yield cache
            version = source.version
