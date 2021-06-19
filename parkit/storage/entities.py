# pylint: disable = c-extension-no-member
#
# reviewed: 6/16/21
#
import codecs
import logging

from typing import (
    Any, Iterator, Optional, Tuple
)

import orjson

from parkit.storage.entity import Entity
from parkit.storage.threadlocal import CursorDict
from parkit.typeddicts import Descriptor
from parkit.utility import create_class

logger = logging.getLogger(__name__)

def load_entity(
    name_db: Any,
    descriptor_db: Any,
    cursors: CursorDict,
    site_uuid: str,
    namespace: str,
    /, *,
    name: Optional[str] = None,
    uuid: Optional[bytes] = None
) -> Optional[Entity]:
    if uuid is None:
        if name is None:
            raise ValueError()
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
                assert name is not None
                return create_class(descriptor['type'])(
                    '/'.join([namespace, name]),
                    site_uuid = site_uuid, create = False,
                    bind = True
                )
            except AttributeError:
                assert name is not None
                return create_class('parkit.storage.entity.Entity')(
                    namespace, name,
                    site_uuid = site_uuid,
                    create = False, bind = True
                )
    return None

def descriptor_iter(
    name_db: Any,
    descriptor_db: Any,
    cursors: CursorDict,
    /, *,
    include_hidden: bool = False
) -> Iterator[Tuple[str, Descriptor]]:
    name_cursor = cursors[name_db]
    descriptor_cursor = cursors[descriptor_db]
    if name_cursor.first():
        while True:
            name = codecs.decode(name_cursor.key(), encoding = 'utf-8')
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
    name_db: Any,
    cursors: CursorDict,
    /, *,
    include_hidden: bool = False
) -> Iterator[str]:
    cursor = cursors[name_db]
    if cursor.first():
        while True:
            name = codecs.decode(cursor.key(), encoding = 'utf-8')
            if include_hidden or not (name.startswith('__') and name.endswith('__')):
                yield name
            if not cursor.next():
                break

def entity_iter(
    name_db: Any,
    descriptor_db: Any,
    cursors: CursorDict,
    site_uuid: str,
    namespace: str,
    /, *,
    include_hidden: bool = False
) -> Iterator[Entity]:
    cursor = cursors[name_db]
    if cursor.first():
        while True:
            name = codecs.decode(cursor.key(), encoding = 'utf-8')
            if include_hidden or not (name.startswith('__') and name.endswith('__')):
                entity = load_entity(
                    name_db, descriptor_db, cursors, site_uuid, namespace,
                    name = name, uuid = cursor.value()
                )
                if entity is not None:
                    yield entity
            if not cursor.next():
                break
