# pylint: disable = broad-except
import logging
import pickle

from typing import (
    Any, ByteString, Callable, cast, Dict, Iterator, Optional
)

import parkit.storage.threadlocal as thread

from parkit.exceptions import ObjectNotFoundError
from parkit.storage import (
    context,
    Entity,
    EntityMeta
)
from parkit.utility import resolve_path

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
        metadata: Optional[Dict[str, Any]] = None
    ):
        if path is not None:
            name, namespace = resolve_path(path)
        else:
            name = namespace = None
        super().__init__(
            name, properties = [], namespace = namespace,
            create = create, bind = bind, versioned = versioned,
            type_check = type_check, metadata = metadata
        )

    def __getattr__(
        self,
        key: str
    ) -> Any:
        if key == '_Entity__def' or key in self._Entity__def:
            raise AttributeError()
        binkey = b''.join([
            self._Entity__uuidbytes,
            self.encode_attr_key(key) if self.encode_attr_key else cast(ByteString, key)
        ])
        try:
            implicit = False
            cursor = None
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin(write = True)
                cursor = txn.cursor(db = self._Entity__attrdb)
            else:
                cursor = thread.local.cursors[id(self._Entity__attrdb)]
            result = None
            if cursor.set_key(binkey):
                result = cursor.value()
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)
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
        binkey = b''.join([
            self._Entity__uuidbytes,
            self.encode_attr_key(key) if self.encode_attr_key else cast(ByteString, key)
        ])
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin(write = True)
            result = txn.delete(key = binkey, db = self._Entity__attrdb)
            if implicit:
                if result and self._Entity__vers:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif self._Entity__vers:
                thread.local.changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)
        if not result:
            if not self.exists:
                raise ObjectNotFoundError()
            raise AttributeError()

    def attributes(self) -> Iterator[str]:
        with context(
            self._Entity__env, write = False,
            inherit = True, buffers = True
        ):
            cursor = thread.local.cursors[id(self._Entity__attrdb)]
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
        binkey = b''.join([
            self._Entity__uuidbytes,
            self.encode_attr_key(key) if self.encode_attr_key else cast(ByteString, key)
        ])
        value = self.encode_attr_value(value) if self.encode_attr_value else value
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin(write = True)
            assert txn.put(
                key = binkey, value = value, overwrite = True, append = False,
                db = self._Entity__attrdb
            )
            if implicit:
                if self._Entity__vers:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif self._Entity__vers:
                thread.local.changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)
