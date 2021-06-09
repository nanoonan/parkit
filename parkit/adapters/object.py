# pylint: disable = broad-except
import logging
import pickle
import typing

from typing import (
    Any, ByteString, Callable, cast, Iterator, List, Optional, Tuple
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.scopetable import add_scope_entry
from parkit.exceptions import ObjectNotFoundError
from parkit.storage.context import transaction_context
from parkit.storage.entity import Entity
from parkit.storage.entitymeta import EntityMeta
from parkit.typeddicts import LMDBProperties
from parkit.utility import (
    envexists,
    getenv,
    resolve_path
)

logger = logging.getLogger(__name__)

class Object(Entity, metaclass = EntityMeta):

    encode_attr_key: Optional[Callable[..., ByteString]] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    decode_attr_key: Optional[Callable[..., ByteString]] = \
    cast(Callable[..., ByteString], staticmethod(pickle.loads))

    encode_attr_value: Optional[Callable[..., ByteString]] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    decode_attr_value: Optional[Callable[..., Any]] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        create: bool = True,
        bind: bool = True,
        versioned: bool = True,
        type_check: bool = True,
        metadata: Optional[typing.Dict[str, Any]] = None,
        site: Optional[str] = None,
        db_properties: Optional[List[LMDBProperties]] = None,
        on_init: Optional[Callable[[bool], None]] = None
    ):
        namespace, name = resolve_path(path)
        super().__init__(
            namespace, name, db_properties = db_properties,
            create = create, bind = bind, versioned = versioned,
            type_check = type_check, metadata = metadata,
            site = site, on_init = on_init,
            anonymous = not bool(path) or path == constants.MEMORY_NAMESPACE
        )

    #
    # anonymous objects weakly referenced on foreign sites
    #

    def __setstate__(self, from_wire: Tuple[str, str, str]):
        super().__setstate__(from_wire)
        if self.anonymous and envexists(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME):
            if self.site_uuid == getenv(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME, str):
                add_scope_entry(
                    self.uuid,
                    self.site_uuid,
                    getenv(constants.PROCESS_UUID_ENVNAME, str)
                )

    def __getstate__(self) -> Tuple[str, str, str]:
        if self.anonymous and envexists(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME):
            if self.site_uuid == getenv(constants.ANONYMOUS_SCOPE_FLAG_ENVNAME, str):
                add_scope_entry(self.uuid, self.site_uuid)
        return super().__getstate__()

    def __getattr__(
        self,
        key: str
    ) -> Any:
        if key == '_Entity__def' or key in self._Entity__def:
            raise AttributeError()
        key_bytes = b''.join([
            self._Entity__uuidbytes,
            self.encode_attr_key(key) if self.encode_attr_key else cast(ByteString, key)
        ])
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self._Entity__env, write = False)
            cursor = cursors[self._Entity__attrdb]
            result = None
            if cursor.set_key(key_bytes):
                result = cursor.value()
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        finally:
            if implicit and cursor:
                cursor.close()
        if result is None:
            if not self.exists:
                raise ObjectNotFoundError()
            raise AttributeError()
        return self.decode_attr_value(result) if self.decode_attr_value else result

    def __delattr__(
        self,
        key: Any
    ):
        if not hasattr(self, '_Entity__def') or key in self._Entity__def:
            super().__delattr__(key)
            return
        key_bytes = b''.join([
            self._Entity__uuidbytes,
            self.encode_attr_key(key) if self.encode_attr_key else cast(ByteString, key)
        ])
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True)
            result = txn.delete(key = key_bytes, db = self._Entity__attrdb)
            if implicit:
                if result and self._Entity__vers:
                    self._Entity__increment_version(cursors)
                txn.commit()
            elif result and self._Entity__vers:
                changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        if not result:
            if not self.exists:
                raise ObjectNotFoundError()
            raise AttributeError()

    def attributes(self) -> Iterator[str]:
        with transaction_context(self._Entity__env, write = False) as (_, cursors, _):
            cursor = cursors[self._Entity__attrdb]
            if cursor.set_range(self._Entity__uuidbytes):
                while True:
                    key = cursor.key()
                    key = bytes(key) if isinstance(key, memoryview) else key
                    if key.startswith(self._Entity__uuidbytes):
                        key = key[len(self._Entity__uuidbytes):]
                        yield self.decode_attr_key(key) if self.decode_attr_key else key
                        if cursor.next():
                            continue
                    return

    def __setattr__(
        self,
        key: Any,
        value: Any
    ):
        if not hasattr(self, '_Entity__def') or key in self._Entity__def:
            super().__setattr__(key, value)
            return
        key_bytes = b''.join([
            self._Entity__uuidbytes,
            self.encode_attr_key(key) if self.encode_attr_key else cast(ByteString, key)
        ])
        value = self.encode_attr_value(value) if self.encode_attr_value else value
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True)
            assert txn.put(
                key = key_bytes, value = value, overwrite = True, append = False,
                db = self._Entity__attrdb
            )
            if implicit:
                if self._Entity__vers:
                    self._Entity__increment_version(cursors)
                txn.commit()
            elif self._Entity__vers:
                changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
