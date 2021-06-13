import logging
import mmap

from typing import (
    Any, Dict, Iterator, Optional, Tuple
)

import cardinality

import parkit.storage.threadlocal as thread

from parkit.exceptions import SiteNotSpecifiedError
from parkit.storage.context import transaction_context
from parkit.storage.entities import (
    descriptor_iter,
    load_entity,
    name_iter,
    entity_iter
)
from parkit.storage.entity import Entity
from parkit.storage.environment import (
    get_environment_threadsafe,
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
        cursize = self.maxsize
        if value == cursize:
            return
        if value < self.maxsize:
            raise ValueError()
        if value % mmap.PAGESIZE != 0:
            raise ValueError()
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
        _, env, name_db, _, _, descriptor_db = get_environment_threadsafe(storage_path, self._path)
        with transaction_context(env, write = False, iterator = True) as (_, cursors, _):
            obj = load_entity(
                name_db, descriptor_db, cursors, get_site_name(self._site_uuid),
                self._path, name = name
            )
            if obj is not None:
                return obj
            raise KeyError()

    def metadata(self, /, *, include_hidden: bool = False) -> Iterator[Tuple[str, Dict[str, Any]]]:
        for name, descriptor in self.descriptors(include_hidden = include_hidden):
            yield (name, descriptor['metadata'])

    def descriptors(self, /, *, include_hidden: bool = False) -> Iterator[Tuple[str, Descriptor]]:
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        _, env, name_db, _, _, descriptor_db = get_environment_threadsafe(storage_path, self._path)
        with transaction_context(env, write = False, iterator = True) as (_, cursors, _):
            return descriptor_iter(name_db, descriptor_db, cursors, include_hidden = include_hidden)

    def names(self, /, *, include_hidden: bool = False) -> Iterator[str]:
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        _, env, name_db, _, _, _ = get_environment_threadsafe(storage_path, self._path)
        with transaction_context(env, write = False, iterator = True) as (_, cursors, _):
            return name_iter(name_db, cursors, include_hidden = include_hidden)

    def entities(self, /, *, include_hidden: bool = False) -> Iterator[Entity]:
        storage_path = StoragePath(site_uuid = self._site_uuid).path
        _, env, name_db, _, _, descriptor_db = get_environment_threadsafe(storage_path, self._path)
        with transaction_context(env, write = False, iterator = True) as (_, cursors, _):
            return entity_iter(
                name_db, descriptor_db, cursors, get_site_name(self._site_uuid), self._path,
                include_hidden = include_hidden
            )

    def __iter__(self) -> Iterator[Entity]:
        return self.entities()

    def __len__(self) -> int:
        return cardinality.count(self.names())

    def __contains__(self, obj) -> bool:
        if isinstance(obj, str):
            return obj in self.names()
        return obj in self.__iter__()
