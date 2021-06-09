import logging

from typing import (
    Any, Dict, Iterator, Optional, Tuple
)

import parkit.storage.threadlocal as thread

from parkit.exceptions import SiteNotSpecifiedError
from parkit.storage.entities import (
    descriptor_iter,
    load_entity,
    name_iter,
    entity_iter
)
from parkit.storage.entity import Entity
from parkit.storage.environment import (
    get_namespace_size,
    set_namespace_size
)
from parkit.storage.site import (
    get_site_name,
    get_site_uuid
)
from parkit.storage.threadlocal import StoragePath
from parkit.typeddicts import Descriptor
from parkit.utility import resolve_namespace

logger = logging.getLogger(__name__)

class Namespace():

    def __init__(
        self,
        namespace: Optional[str] = None,
        /, *,
        site: Optional[str] = None
    ):
        self._path = resolve_namespace(namespace)
        if site:
            self._site_uuid = get_site_uuid(site)
        elif thread.local.storage_path:
            self._site_uuid = thread.local.storage_path.site_uuid
        else:
            raise SiteNotSpecifiedError()

    @property
    def site(self) -> str:
        return get_site_name(self._site_uuid)

    @property
    def site_uuid(self) -> str:
        return self._site_uuid

    @property
    def path(self) -> str:
        return self._path

    @property
    def storage_path(self) -> str:
        return StoragePath(site_uuid = self._site_uuid).path

    @property
    def maxsize(self) -> int:
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        return get_namespace_size(storage_path, self._path)

    @maxsize.setter
    def maxsize(self, value: int):
        assert value > self.maxsize
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        set_namespace_size(value, storage_path, self._path)

    def __delitem__(
        self,
        name: str
    ):
        self.__getitem__(name).drop()

    def __getitem__(
        self,
        name: str
    ) -> Entity:
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        obj = load_entity(get_site_name(self._site_uuid), storage_path, self._path, name)
        if obj is not None:
            return obj
        raise KeyError()

    def metadata(self, /, *, include_hidden: bool = False) \
    -> Iterator[Tuple[str, Dict[str, Any]]]:
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        for name, descriptor in descriptor_iter(
            storage_path, self._path, include_hidden = include_hidden
        ):
            yield (name, descriptor['metadata'])

    def descriptors(self, /, *, include_hidden: bool = False) \
    -> Iterator[Tuple[str, Descriptor]]:
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        return descriptor_iter(
            storage_path, self._path, include_hidden = include_hidden
        )

    def names(self, /, *, include_hidden: bool = False) -> Iterator[str]:
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        return name_iter(storage_path, self._path, include_hidden = include_hidden)

    def entities(self, /, *, include_hidden: bool = False) -> Iterator[Entity]:
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        return entity_iter(
            get_site_name(self._site_uuid),
            storage_path, self._path,
            include_hidden = include_hidden
        )

    def __iter__(self) -> Iterator[Entity]:
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        return entity_iter(get_site_name(self._site_uuid), storage_path, self._path)

    def __len__(self) -> int:
        return len(list(self.names()))

    def __contains__(self, obj) -> bool:
        if isinstance(obj, str):
            return obj in list(self.names())
        return obj in list(self.__iter__())
