# pylint: disable = c-extension-no-member
import logging

from typing import (
    Any, Iterator, Optional, Tuple
)

import lmdb
import orjson

from parkit.storage.context import (
    InheritMode,
    transaction_context
)
from parkit.storage.entity import Entity
from parkit.storage.environment import get_environment_threadsafe
from parkit.typeddicts import Descriptor
from parkit.utility import create_class

logger = logging.getLogger(__name__)

def load_entity(
    site: str,
    storage_path: str,
    namespace: str,
    name: str,
    /, *,
    uuid: Optional[bytes] = None,
    env: Optional[lmdb.Environment] = None,
    descriptor_db: Optional[Any] = None
) -> Optional[Entity]:
    if uuid is None:
        _, env, name_db, _, _, descriptor_db = get_environment_threadsafe(
            storage_path, namespace
        )
    with transaction_context(
        env, write = True, inherit = InheritMode.WriteExclusive
    ) as (_, cursors, _):
        if uuid is None:
            cursor = cursors[name_db]
            uuid = cursor.get(key = name.encode('utf-8'))
        if uuid is not None:
            cursor = cursors[descriptor_db]
            data = cursor.get(key = uuid)
            if data is not None:
                descriptor = orjson.loads(
                    bytes(data) if isinstance(data, memoryview) else data
                )
                try:
                    return create_class(descriptor['type'])(
                        '/'.join([namespace, name]),
                        site = site
                    )
                except AttributeError:
                    return create_class('parkit.storage.Entity')(
                        '/'.join([namespace, name]), type_check = False,
                        site = site
                    )
    return None

def descriptor_iter(
    storage_path: str,
    namespace: str,
    /, *,
    include_hidden: bool = False
) -> Iterator[Tuple[str, Descriptor]]:
    _, env, name_db, _, _, descriptor_db = get_environment_threadsafe(
        storage_path, namespace
    )
    with transaction_context(env, write = False) as (_, cursors, _):
        name_cursor = cursors[name_db]
        descriptor_cursor = cursors[descriptor_db]
        if name_cursor.first():
            while True:
                key = name_cursor.key()
                name = bytes(key).decode('utf-8') if isinstance(key, memoryview) else \
                key.decode('utf-8')
                if include_hidden or not (name.startswith('__') and name.endswith('__')):
                    uuid = name_cursor.value()
                    assert uuid is not None
                    data = descriptor_cursor.get(uuid)
                    assert data is not None
                    descriptor = orjson.loads(
                        bytes(data) if isinstance(data, memoryview) else data
                    )
                    yield (name, descriptor)
                if not name_cursor.next():
                    break

def name_iter(
    storage_path: str,
    namespace: str,
    /, *,
    include_hidden: bool = False
) -> Iterator[str]:
    _, env, name_db, _, _, _ = get_environment_threadsafe(storage_path, namespace)
    with transaction_context(env, write = False) as (_, cursors, _):
        cursor = cursors[name_db]
        if cursor.first():
            while True:
                key = cursor.key()
                name = bytes(key).decode('utf-8') if isinstance(key, memoryview) else \
                key.decode('utf-8')
                if include_hidden or not (name.startswith('__') and name.endswith('__')):
                    yield name
                if not cursor.next():
                    break

def entity_iter(
    site: str,
    storage_path: str,
    namespace: str,
    /, *,
    include_hidden: bool = False
) -> Iterator[Entity]:
    _, env, name_db, _, _, descriptor_db = get_environment_threadsafe(storage_path, namespace)
    with transaction_context(
        env, write = True, inherit = InheritMode.WriteExclusive
    ) as (_, cursors, _):
        cursor = cursors[name_db]
        if cursor.first():
            while True:
                key = cursor.key()
                name = bytes(key).decode('utf-8') if isinstance(key, memoryview) else \
                key.decode('utf-8')
                if include_hidden or not (name.startswith('__') and name.endswith('__')):
                    entity = load_entity(
                        site, storage_path, namespace, name, uuid = cursor.value(),
                        env = env, descriptor_db = descriptor_db
                    )
                    if entity is not None:
                        yield entity
                if not cursor.next():
                    break
