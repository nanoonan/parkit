# pylint: disable = broad-except, no-value-for-parameter, non-parent-init-called, super-init-not-called, unused-import
import collections.abc
import logging
import pickle
import types
import typing

from typing import (
    Any, ByteString, Callable, cast, Iterable, Iterator, MutableMapping,
    Optional, Tuple, Union
)

import parkit.storage.threadlocal as thread

from parkit.adapters.sized import Sized
from parkit.storage import (
    Entity,
    EntityMeta,
    context,
    Missing
)
from parkit.utility import (
    compile_function,
    resolve_path
)

logger = logging.getLogger(__name__)

_unspecified_class = types.new_class('__unspecified__')

def mkiter(
    keys: bool = True,
    values: bool = False
) -> Tuple[str, Callable[..., Iterator[Union[Any, Tuple[Any, Any]]]]]:
    code = """
def method(self) -> Iterator[Union[Any, Tuple[Any, Any]], None, None]:
    with context(
        self._Entity__env, write = False,
        inherit = True, buffers = True
    ):
        cursor = thread.local.cursors[id(self._Entity__userdb[0])]
        if not cursor.first():
            return
        while True:
            {0}
            if not cursor.next():
                return
"""
    if keys and not values:
        insert = """
            yield self.decode_key(cursor.key()) if self.decode_key else cursor.key()
        """.strip()
    elif values and not keys:
        insert = """
            yield self.decode_value(cursor.value()) if self.decode_value else cursor.value()
        """
    else:
        insert = """
            yield (
                self.decode_key(cursor.key()) if self.decode_key else cursor.key(),
                self.decode_value(cursor.value()) if self.decode_value else cursor.value()
            )
        """
    return (code.format(insert), compile_function(
        code, insert, glbs = globals()
    ))

class DictMeta(EntityMeta):

    def __initialize_class__(cls):
        if isinstance(cast(Dict, cls).__iter__, Missing):
            code, method = mkiter(keys = True, values = False)
            setattr(cls, '__iter__', method)
            setattr(cls, '__iter__code', code)
        if isinstance(cast(Dict, cls).keys, Missing):
            code, method = mkiter(keys = True, values = False)
            setattr(cls, 'keys', method)
            setattr(cls, 'keyscode', code)
        if isinstance(cast(Dict, cls).values, Missing):
            code, method = mkiter(keys = False, values = True)
            setattr(cls, 'values', method)
            setattr(cls, 'valuescode', code)
        if isinstance(cast(Dict, cls).items, Missing):
            code, method = mkiter(keys = True, values = True)
            setattr(cls, 'items', method)
            setattr(cls, 'itemscode', code)
        super().__initialize_class__()

class Dict(Sized, metaclass = DictMeta):

    get_metadata: Optional[Callable[..., Any]] = None

    decode_key: Optional[Callable[..., Any]] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encode_key: Optional[Callable[..., ByteString]] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    decode_value: Optional[Callable[..., Any]] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encode_value: Optional[Callable[..., ByteString]] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    def __init__(
        self,
        path: Optional[str] = None,
        /,*,
        create: bool = True,
        bind: bool = True,
        type_check: bool = True,
        versioned: bool = True,
        metadata: Optional[typing.Dict[str, Any]] = None
    ):
        if path is not None:
            name, namespace = resolve_path(path)
        else:
            name = namespace = None
        Entity.__init__(
            self, name, properties = [{}, {}], namespace = namespace,
            create = create, bind = bind, versioned = versioned,
            type_check = type_check, metadata = metadata
        )

    def __getitem__(
        self,
        key: Any,
        /
    ) -> Any:
        key = self.encode_key(key) if self.encode_key else key
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin()
            result = txn.get(key = key, default = None, db = self._Entity__userdb[0])
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)
        if result is None:
            raise KeyError()
        return self.decode_value(result) if self.decode_value else result

    def get(
        self,
        key: Any,
        default: Any = None,
        /
    ) -> Any:
        key = self.encode_key(key) if self.encode_key else key
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin()
            result = txn.get(key = key, default = default, db = self._Entity__userdb[0])
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)
        return self.decode_value(result) if self.decode_value and result != default else result

    def setdefault(
        self,
        key: Any,
        default: Any = None,
        /
    ) -> Any:
        key = self.encode_key(key) if self.encode_key else key
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._Entity__userdb[0])]
            if not cursor:
                txn = self._Entity__env.begin(write = True)
                cursor = txn.cursor(db = self._Entity__userdb[0])
            result = cursor.set_key(key)
            if result:
                value = cursor.value()
            else:
                default = self.encode_value(default) if self.encode_value else default
                assert cursor.put(key = key, value = default)
                if txn and self._Entity__vers:
                    self.increment_version(use_transaction = txn)
                elif not txn and self._Entity__vers:
                    thread.local.changed.add(self)
            if txn:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn)
        finally:
            if txn and cursor:
                cursor.close()
        return default if not result else (self.decode_value(value) if self.decode_value else value)

    def popitem(self) -> Any:
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._Entity__userdb[0])]
            if not cursor:
                txn = self._Entity__env.begin(write = True)
                cursor = txn.cursor(db = self._Entity__userdb[0])
            result = cursor.last()
            if result:
                key = cursor.key()
                value = cursor.pop(key)
                if txn and self._Entity__vers:
                    self.increment_version(use_transaction = txn)
                elif not txn and self._Entity__vers:
                    thread.local.changed.add(self)
            if txn:
                txn.commit()
            if not result:
                raise KeyError()
        except BaseException as exc:
            self._Entity__abort(exc, txn)
        finally:
            if txn and cursor:
                cursor.close()
        return (
            self.decode_key(key) if self.decode_key else key,
            self.decode_value(value) if self.decode_value else value
        )

    def pop(
        self,
        key: Any,
        default: Any = _unspecified_class(),
        /
    ) -> Any:
        key = self.encode_key(key) if self.encode_key else key
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._Entity__userdb[0])]
            if not cursor:
                txn = self._Entity__env.begin(write = True)
                cursor = txn.cursor(db = self._Entity__userdb[0])
            result = cursor.pop(key)
            if txn:
                if result is not None and self._Entity__vers:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif result is not None and self._Entity__vers:
                thread.local.changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn)
        finally:
            if txn and cursor:
                cursor.close()
        if result is None and isinstance(default, _unspecified_class):
            raise KeyError()
        if result is None:
            return default
        return self.decode_value(result) if self.decode_value else result

    def __delitem__(
        self,
        key: Any,
        /
    ):
        key = self. encode_key(key) if self.encode_key else key
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin(write = True)
            result = txn.delete(key = key, db = self._Entity__userdb[0])
            if implicit:
                if result and self._Entity__vers:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif self._Entity__vers:
                thread.local.changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)

    def __contains__(
        self,
        key: Any,
        /
    ) -> bool:
        key = self.encode_key(key) if self.encode_key else key
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._Entity__userdb[0])]
            if not cursor:
                txn = self._Entity__env.begin()
                cursor = txn.cursor(db = self._Entity__userdb[0])
            result = cursor.set_key(key)
            if txn:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn)
        finally:
            if txn and cursor:
                cursor.close()
        return result

    def __setitem__(
        self,
        key: Any,
        value: Any,
        /
    ):
        key = self.encode_key(key) if self.encode_key else key
        value = self.encode_value(value) if self.encode_value else value
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin(write = True)
            result = txn.put(
                key = key, value = value, overwrite = True, append = False,
                db = self._Entity__userdb[0]
            )
            if implicit:
                if result and self._Entity__vers:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif result and self._Entity__vers:
                thread.local.changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)

    def update(
        self,
        *args: Union[
            Tuple[()],
            Tuple[Union[typing.Dict[Any, Any], MutableMapping[Any, Any], Iterable[Tuple[Any, Any]]]]
        ],
        **kwargs: typing.Dict[Any, Any]
    ):
        dict_items = iter_items = None
        consumed = added = 0
        if args and isinstance(args[0], dict):
            dict_items = [
                (
                    self.encode_key(key) if self.encode_key else key,
                    self.encode_value(value) if self.encode_value else value
                )
                for key, value in args[0].items()
            ]
        elif args and isinstance(args[0], collections.abc.MutableMapping):
            iter_items = [
                (
                    self.encode_key(key) if self.encode_key else key,
                    self.encode_value(value) if self.encode_value else value
                )
                for key, value in args[0].items()
            ]
        elif args:
            iter_items = [
                (
                    self.encode_key(key) if self.encode_key else key,
                    self.encode_value(value) if self.encode_value else value
                )
                for key, value in args[0]
            ]
        kwargs_items = [
            (
                self.encode_key(key) if self.encode_key else key,
                self.encode_value(value) if self.encode_value else value
            )
            for key, value in kwargs.items()
        ]
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._Entity__userdb[0])]
            if not cursor:
                txn = self._Entity__env.begin(write = True)
                cursor = txn.cursor(db = self._Entity__userdb[0])
            if dict_items:
                cons, add = cursor.putmulti(dict_items)
                consumed += cons
                added += add
            if kwargs_items:
                cons, add = cursor.putmulti(kwargs_items)
                consumed += cons
                added += add
            if iter_items:
                cons, add = cursor.putmulti(iter_items)
                consumed += cons
                added += add
            if txn:
                if added and self._Entity__vers:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif added and self._Entity__vers:
                thread.local.changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn)
        finally:
            if txn and cursor:
                cursor.close()

    __iter__: Callable[..., Iterator[Union[Any, Tuple[Any, Any]]]] = Missing()

    keys: Callable[..., Iterator[Union[Any, Tuple[Any, Any]]]] = Missing()

    values: Callable[..., Iterator[Union[Any, Tuple[Any, Any]]]] = Missing()

    items: Callable[..., Iterator[Union[Any, Tuple[Any, Any]]]] = Missing()
