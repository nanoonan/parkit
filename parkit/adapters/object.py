# pylint: disable = broad-except
import logging
import pickle

from typing import (
    Any, ByteString, Callable, cast, Generator, Optional, Tuple
)

import parkit.storage.threadlocal as thread

from parkit.storage import (
    context,
    Entity,
    EntityMeta
)
from parkit.utility import resolve_path

logger = logging.getLogger(__name__)

class ObjectMeta(EntityMeta):
    pass

class Object(Entity, metaclass = ObjectMeta):

    encattrkey: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    decattrkey: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.loads))

    encattrval: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    decattrval: Callable[..., Any] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    def __init__(
        self,
        path: str,
        /, *,
        create: bool = True,
        bind: bool = True,
        versioned: bool = True,
        on_create: Optional[Callable[[], None]] = None
    ) -> None:
        name, namespace = resolve_path(path)
        super().__init__(
            name, properties = [], namespace = namespace,
            create = create, bind = bind, versioned = versioned,
            on_create = on_create
        )

    def __getattr__(
        self,
        key: Any,
        /
    ) -> Any:
        if key[0] == '_' or key in self._Entity__def:
            raise AttributeError()
        if key is not None:
            key = self.encattrkey(key) if self.encattrkey else key
            key = b''.join([self._Entity__uuidbytes, key])
        else:
            key = self._Entity__uuidbytes
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
            if cursor.set_key(key):
                result = cursor.value()
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)
        finally:
            if implicit and cursor:
                cursor.close()
        if result is None:
            raise AttributeError()
        return self.decattrval(result) if self.decattrval else result

    def __delattr__(
        self,
        key: Any,
        /
    ) -> None:
        if key[0] == '_' or key in self._Entity__def:
            super().__delattr__(key)
            return
        if key is not None:
            key = self.encattrkey(key) if self.encattrkey else key
            key = b''.join([self._Entity__uuidbytes, key])
        else:
            key = self._Entity__uuidbytes
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin(write = True)
            result = txn.delete(key = key, db = self._Entity__attrdb)
            if implicit:
                if result and self._Entity__vers:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif self._Entity__vers:
                thread.local.changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)

    def attributes(
        self
    ) -> Generator[Tuple[Any, Any], None, None]:
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
                        yield (
                            self.decattrkey(key) if self.decattrkey else key,
                            self.decattrval(cursor.value()) if self.decattrval else cursor.value()
                        )
                        if cursor.next():
                            continue
                    return

    def __setattr__(
        self,
        key: Any,
        value: Any,
        /
    ) -> None:
        if key[0] == '_' or key in self._Entity__def:
            super().__setattr__(key, value)
            return
        if key is not None:
            key = self.encattrkey(key) if self.encattrkey else key
            key = b''.join([self._Entity__uuidbytes, key])
        else:
            key = self._Entity__uuidbytes
        value = self.encattrval(value) if self.encattrval else value
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin(write = True)
            assert txn.put(
                key = key, value = value, overwrite = True, append = False,
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
