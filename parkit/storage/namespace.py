import logging
import os

from typing import (
    Any, cast, Dict, Iterator, Optional, Tuple
)

import parkit.constants as constants

from parkit.storage.environment import (
    get_namespace_size,
    set_namespace_size
)
from parkit.storage.objects import (
    descriptor_iter,
    load_object,
    name_iter,
    object_iter
)
from parkit.typeddicts import Descriptor
from parkit.utility import (
    getenv,
    resolve_namespace
)

logger = logging.getLogger(__name__)

class Namespace():

    def __init__(self, namespace: Optional[str] = None):
        self._path = cast(
            str,
            resolve_namespace(namespace) if namespace else constants.DEFAULT_NAMESPACE
        )

    @property
    def dir(self) -> str:
        return os.path.join(
            getenv(constants.STORAGE_PATH_ENVNAME, str),
            self._path
        )

    @property
    def path(self) -> str:
        return self._path

    @property
    def maxsize(self) -> int:
        return get_namespace_size(self._path)

    @maxsize.setter
    def maxsize(self, value: int):
        assert value > 0
        set_namespace_size(value, namespace = self._path)

    def __delitem__(
        self,
        name: str
    ):
        self.__getitem__(name).drop()

    def __getitem__(
        self,
        name: str
    ) -> Any:
        obj = load_object(self._path, name)
        if obj is not None:
            return obj
        raise KeyError()

    def metadata(self) -> Iterator[Tuple[str, Dict[str, Any]]]:
        for name, descriptor in descriptor_iter(self._path):
            yield (name, descriptor['custom'] if 'custom' in descriptor else {})

    def descriptors(self) -> Iterator[Tuple[str, Descriptor]]:
        return descriptor_iter(self._path)

    def names(self) -> Iterator[str]:
        return name_iter(self._path)

    def objects(self) -> Iterator[Any]:
        return self.__iter__()

    def __iter__(self) -> Iterator[Any]:
        return object_iter(self._path)

    def __len__(self) -> int:
        return len(list(self.names()))

def namespaces() -> Iterator[Namespace]:
    for folder, _, _ in os.walk(getenv(constants.STORAGE_PATH_ENVNAME, str)):
        if folder != getenv(constants.STORAGE_PATH_ENVNAME, str):
            top_level_namespace = \
            folder[len(getenv(constants.STORAGE_PATH_ENVNAME, str)):].split(os.path.sep)[1]
            if not (
                top_level_namespace.startswith('__') and top_level_namespace.endswith('__')
            ):
                yield Namespace('/'.join(
                    folder[len(getenv(constants.STORAGE_PATH_ENVNAME, str)):].split(os.path.sep)[1:]
                ))
