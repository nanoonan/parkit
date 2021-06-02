# pylint: disable = c-extension-no-member
import logging

from typing import (
    Any, Iterator, Optional, Tuple
)

import orjson

import parkit.storage.threadlocal as thread

from parkit.storage.context import context
from parkit.storage.environment import get_environment_threadsafe
from parkit.typeddicts import Descriptor
from parkit.utility import create_class

logger = logging.getLogger(__name__)

def load_object(
    storage_path: str,
    namespace: str,
    name: str
) -> Optional[Any]:
    env, name_db, _, _, descriptor_db = get_environment_threadsafe(
        storage_path, namespace
    )
    with context(env, write = True, inherit = True, buffers = False):
        cursor = thread.local.cursors[id(name_db)]
        uuid = cursor.get(name.encode('utf-8'))
        if uuid is not None:
            cursor = thread.local.cursors[id(descriptor_db)]
            data = cursor.get(uuid)
            if data is not None:
                descriptor = orjson.loads(
                    bytes(data) if isinstance(data, memoryview) else data
                )
                try:
                    return create_class(
                        descriptor['type'])('/'.join([namespace, name]),
                        storage_path = storage_path
                    )
                except AttributeError:
                    return create_class('parkit.adapters.Object')(
                        '/'.join([namespace, name]), type_check = False,
                        storage_path = storage_path
                    )
    return None

def descriptor_iter(
    storage_path: str,
    namespace: str
) -> Iterator[Tuple[str, Descriptor]]:
    descriptors = []
    env, name_db, _, _, descriptor_db = get_environment_threadsafe(
        storage_path, namespace
    )
    with context(env, write = False, inherit = True, buffers = False):
        name_cursor = thread.local.cursors[id(name_db)]
        descriptor_cursor = thread.local.cursors[id(descriptor_db)]
        if name_cursor.first():
            while True:
                key = name_cursor.key()
                name = bytes(key).decode('utf-8') if isinstance(key, memoryview) else \
                key.decode('utf-8')
                if not (name.startswith('__') and name.endswith('__')):
                    uuid = name_cursor.value()
                    assert uuid is not None
                    data = descriptor_cursor.get(uuid)
                    assert data is not None
                    descriptor = orjson.loads(
                        bytes(data) if isinstance(data, memoryview) else data
                    )
                    descriptors.append((name, descriptor))
                if not name_cursor.next():
                    break
    for descriptor in descriptors:
        yield descriptor

def name_iter(
    storage_path: str,
    namespace: str
) -> Iterator[str]:
    names = []
    env, name_db, _, _, _ = get_environment_threadsafe(storage_path, namespace)
    with context(env, write = False, inherit = True, buffers = False):
        cursor = thread.local.cursors[id(name_db)]
        if cursor.first():
            while True:
                key = cursor.key()
                name = bytes(key).decode('utf-8') if isinstance(key, memoryview) else \
                key.decode('utf-8')
                if not (name.startswith('__') and name.endswith('__')):
                    names.append(name)
                if not cursor.next():
                    break
    for name in names:
        yield name

def object_iter(
    storage_path: str,
    namespace: str
) -> Iterator[Any]:
    for name in name_iter(storage_path, namespace):
        obj = load_object(storage_path, namespace, name)
        if obj is not None:
            yield obj
