#
# reviewed: 6/16/21
#
import logging

from typing import (
    Any, Dict, Iterator, Optional, Tuple
)

import parkit.storage.threadlocal as thread

from parkit.exceptions import (
    ObjectNotFoundError,
    SiteNotSpecifiedError
)
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
from parkit.storage.site import get_storage_path
from parkit.typeddicts import Descriptor
from parkit.utility import (
    get_pagesize,
    resolve_namespace
)

logger = logging.getLogger(__name__)

class Namespace():

    def __init__(
        self,
        namespace: Optional[str] = None,
        /, *,
        create: bool = False,
        site_uuid: Optional[str] = None,
        include_hidden: bool = False
    ):
        self._path = resolve_namespace(namespace)
        self._create = create
        self._include_hidden = include_hidden
        if site_uuid is not None:
            self._site_uuid = site_uuid
            self._storage_path = get_storage_path(self._site_uuid)
        else:
            if thread.local.default_site is not None:
                self._storage_path, self._site_uuid = thread.local.default_site
            else:
                raise SiteNotSpecifiedError()

    @property
    def site_uuid(self) -> str:
        return self._site_uuid

    @property
    def path(self) -> str:
        return self._path

    @property
    def storage_path(self) -> str:
        return self._storage_path

    @property
    def maxsize(self) -> int:
        return get_namespace_size(self._storage_path, self._path)

    @maxsize.setter
    def maxsize(self, value: int):
        cursize = self.maxsize
        if value == cursize:
            return
        if value % get_pagesize() != 0:
            raise ValueError()
        set_namespace_size(value, self._storage_path, self._path)

    def __delitem__(
        self,
        name: str
    ):
        self.get(name).drop()

    def __getitem__(
        self,
        name: str
    ) -> Entity:
        return self.get(name)

    def get(
        self,
        name: str
    ):
        _, env, name_db, _, _, descriptor_db = get_environment_threadsafe(
            self._storage_path, self._path, create = self._create
        )
        with transaction_context(env, write = False, iterator = True) as (_, cursors, _):
            obj = load_entity(
                name_db, descriptor_db, cursors, self._site_uuid,
                self._path, name = name
            )
            if obj is not None:
                return obj
            raise ObjectNotFoundError()

    def metadata(
        self,
        /, *,
        include_hidden: Optional[bool] = None
    ) -> Iterator[Tuple[str, Dict[str, Any]]]:
        for name, descriptor in self.descriptors(
            include_hidden = include_hidden if include_hidden is not None else self._include_hidden
        ):
            yield (name, descriptor['metadata'])

    def descriptors(
        self,
        /, *,
        include_hidden: Optional[bool] = None
    ) -> Iterator[Tuple[str, Descriptor]]:
        _, env, name_db, _, _, descriptor_db = get_environment_threadsafe(
            self._storage_path, self._path, create = self._create
        )
        with transaction_context(env, write = False, iterator = True) as (_, cursors, _):
            return descriptor_iter(
                name_db,
                descriptor_db,
                cursors,
                include_hidden = include_hidden \
                if include_hidden is not None else self._include_hidden
            )

    def names(
        self,
        /, *,
        include_hidden: Optional[bool] = None
    ) -> Iterator[str]:
        _, env, name_db, _, _, _ = get_environment_threadsafe(
            self._storage_path, self._path, create = self._create
        )
        with transaction_context(env, write = False, iterator = True) as (_, cursors, _):
            return name_iter(
                name_db, cursors,
                include_hidden = include_hidden \
                if include_hidden is not None else self._include_hidden
            )

    def entities(
        self,
        /, *,
        include_hidden: Optional[bool] = None
    ) -> Iterator[Entity]:
        _, env, name_db, _, _, descriptor_db = get_environment_threadsafe(
            self._storage_path, self._path, create = self._create
        )
        with transaction_context(env, write = False, iterator = True) as (_, cursors, _):
            return entity_iter(
                name_db, descriptor_db, cursors, self._site_uuid, self._path,
                include_hidden = include_hidden \
                if include_hidden is not None else self._include_hidden
            )

    def __iter__(self) -> Iterator[Entity]:
        return self.entities()
