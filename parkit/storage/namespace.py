import logging
import os

from typing import (
    Any, cast, Dict, Iterator, Optional, Tuple
)

import parkit.constants as constants

from parkit.storage.environment import (
    get_environment_threadsafe,
    get_namespace_size,
    resolve_storage_path,
    set_namespace_size
)
from parkit.storage.objects import (
    descriptor_iter,
    load_object,
    name_iter,
    object_iter
)
from parkit.typeddicts import Descriptor
from parkit.utility import resolve_namespace

logger = logging.getLogger(__name__)

class Namespace():

    def __init__(
        self,
        namespace: Optional[str] = None,
        /, *,
        storage_path: Optional[str] = None
    ):
        self._path = cast(
            str,
            resolve_namespace(namespace) if namespace else constants.DEFAULT_NAMESPACE
        )
        self._storage_path = resolve_storage_path(storage_path)
        get_environment_threadsafe(self._storage_path, self._path)

    @property
    def storage_path(self) -> str:
        return self._storage_path

    @property
    def path(self) -> str:
        return self._path

    @property
    def maxsize(self) -> int:
        return get_namespace_size(self._storage_path, self._path)

    @maxsize.setter
    def maxsize(self, value: int):
        assert value > 0
        set_namespace_size(value, self._storage_path, self._path)

    def __delitem__(
        self,
        name: str
    ):
        self.__getitem__(name).drop()

    def __getitem__(
        self,
        name: str
    ) -> Any:
        obj = load_object(self._storage_path, self._path, name)
        if obj is not None:
            return obj
        raise KeyError()

    def metadata(self) -> Iterator[Tuple[str, Dict[str, Any]]]:
        for name, descriptor in descriptor_iter(self._storage_path, self._path):
            yield (name, descriptor['custom'] if 'custom' in descriptor else {})

    def descriptors(self) -> Iterator[Tuple[str, Descriptor]]:
        return descriptor_iter(self._storage_path, self._path)

    def names(self) -> Iterator[str]:
        return name_iter(self._storage_path, self._path)

    def objects(self) -> Iterator[Any]:
        return self.__iter__()

    def __iter__(self) -> Iterator[Any]:
        return object_iter(self._storage_path, self._path)

    def __len__(self) -> int:
        return len(list(self.names()))

def namespaces(storage_path: Optional[str] = None) -> Iterator[Namespace]:
    path = resolve_storage_path(storage_path)
    for folder, _, _ in os.walk(path):
        if folder != path:
            top_level_namespace = \
            folder[len(path):].split(os.path.sep)[1]
            if not (
                top_level_namespace.startswith('__') and top_level_namespace.endswith('__')
            ):
                yield Namespace('/'.join(
                    folder[len(path):].split(os.path.sep)[1:]
                ))
