# pylint: disable = broad-except
#
# reviewed: 6/14/21
#
import logging
import pickle

from typing import (
    Any, Dict, ByteString, Callable, cast, Iterator, List, Optional
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.exceptions import ObjectNotFoundError
from parkit.storage.context import transaction_context
from parkit.storage.entity import Entity
from parkit.storage.entitymeta import EntityMeta
from parkit.typeddicts import LMDBProperties
from parkit.utility import resolve_path

logger = logging.getLogger(__name__)

class Object(Entity, metaclass = EntityMeta):

    encode_attr_key: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    decode_attr_key: Callable[..., str] = \
    cast(Callable[..., str], staticmethod(pickle.loads))

    encode_attr_value: Optional[Callable[..., ByteString]] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    decode_attr_value: Optional[Callable[..., Any]] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        versioned: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
        site_uuid: Optional[str] = None,
        db_properties: Optional[List[LMDBProperties]] = None,
        on_init: Optional[Callable[[bool], None]] = None,
        create: bool = False,
        bind: bool = True
    ):
        namespace, name, anonymous = resolve_path(path)
        super().__init__(
            namespace, name, db_properties = db_properties,
            versioned = versioned, metadata = metadata,
            site_uuid = site_uuid, on_init = on_init,
            create = True if anonymous else create,
            bind = False if anonymous else bind
        )

    def __getattr__(
        self,
        key: str
    ) -> Any:
        if key == '_Entity__def' or key in self._Entity__def:
            raise AttributeError()
        key_bytes = b''.join([
            self._uuid_bytes,
            self.encode_attr_key(key)
        ])
        try:
            txn, _, _, implicit = \
            thread.local.context.get(self._env, write = False)
            result = txn.get(key = key_bytes, db = self._attrdb)
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._abort(exc, txn, implicit)
        if result is None:
            if not self.exists:
                raise ObjectNotFoundError()
            raise AttributeError()
        return self.decode_attr_value(result) if self.decode_attr_value and \
        not key.endswith(constants.KEY_SUFFIX_OBJECT_BINARY_ATTRIBUTE) else result

    def __delattr__(
        self,
        key: str
    ):
        if not hasattr(self, '_Entity__def') or key in self._Entity__def:
            super().__delattr__(key)
            return
        key_bytes = b''.join([
            self._uuid_bytes,
            self.encode_attr_key(key)
        ])
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._env, write = True)
            result = txn.delete(key = key_bytes, db = self._attrdb)
            if implicit:
                if result:
                    self._increment_version(cursors)
                txn.commit()
            elif result:
                changed.add(self)
        except BaseException as exc:
            self._abort(exc, txn, implicit)
        if not result:
            if not self.exists:
                raise ObjectNotFoundError()
            raise AttributeError()

    @property
    def attributes(self) -> Iterator[str]:
        with transaction_context(
            self._env, write = False, iterator = True
        ) as (_, cursors, _):
            cursor = cursors[self._attrdb]
            if cursor.set_range(self._uuid_bytes):
                while True:
                    key = cursor.key()
                    key = bytes(key) if isinstance(key, memoryview) else key
                    if key.startswith(self._uuid_bytes):
                        key = key[len(self._uuid_bytes):]
                        yield self.decode_attr_key(key)
                        if cursor.next():
                            continue
                    return

    def __setattr__(
        self,
        key: str,
        value: Any
    ):
        if not hasattr(self, '_Entity__def') or key in self._Entity__def:
            super().__setattr__(key, value)
            return
        key_bytes = b''.join([
            self._uuid_bytes,
            self.encode_attr_key(key)
        ])
        value = self.encode_attr_value(value) if self.encode_attr_value and \
        not key.endswith(constants.KEY_SUFFIX_OBJECT_BINARY_ATTRIBUTE) else value
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._env, write = True)
            assert txn.put(
                key = key_bytes, value = value, overwrite = True, append = False,
                db = self._attrdb
            )
            if implicit:
                self._increment_version(cursors)
                txn.commit()
            else:
                changed.add(self)
        except BaseException as exc:
            self._abort(exc, txn, implicit)
