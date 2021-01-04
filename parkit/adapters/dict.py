# pylint: disable = broad-except, no-value-for-parameter, non-parent-init-called, super-init-not-called, unused-import
import collections.abc
import logging
import pickle
import types
import typing

from typing import (
    Any, ByteString, Callable, cast, Generator, Iterable, MutableMapping,
    Tuple, Union
)

import parkit.storage.threadlocal as thread

from parkit.adapters.object import ObjectMeta
from parkit.adapters.sized import Sized
from parkit.storage import (
    Entity,
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
) -> Tuple[str, Callable[..., Generator[Union[Any, Tuple[Any, Any]], None, None]]]:
    code = """
def method(self) -> Generator[Union[Any, Tuple[Any, Any]], None, None]:
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
            yield self.decitemkey(cursor.key()) if self.decitemkey else cursor.key()
        """.strip()
    elif values and not keys:
        insert = """
            yield self.decitemval(cursor.value()) if self.decitemval else cursor.value()
        """
    else:
        insert = """
            yield (
                self.decitemkey(cursor.key()) if self.decitemkey else cursor.key(),
                self.decitemval(cursor.value()) if self.decitemval else cursor.value()
            )
        """
    return (code.format(insert), compile_function(
        code, insert, glbs = globals()
    ))

class DictMeta(ObjectMeta):

    def __initialize_class__(cls) -> None:
        if isinstance(cls.__iter__, Missing):
            code, method = mkiter(keys = True, values = False)
            setattr(cls, '__iter__', method)
            setattr(cls, '__iter__code', code)
        if isinstance(cls.keys, Missing):
            code, method = mkiter(keys = True, values = False)
            setattr(cls, 'keys', method)
            setattr(cls, 'keyscode', code)
        if isinstance(cls.values, Missing):
            code, method = mkiter(keys = False, values = True)
            setattr(cls, 'values', method)
            setattr(cls, 'valuescode', code)
        if isinstance(cls.items, Missing):
            code, method = mkiter(keys = True, values = True)
            setattr(cls, 'items', method)
            setattr(cls, 'itemscode', code)
        super().__initialize_class__()

class Dict(Sized, metaclass = DictMeta):

    decitemkey: Callable[..., Any] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encitemkey: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    decitemval: Callable[..., Any] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encitemval: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    def __init__(
        self,
        path: str,
        /,*,
        create: bool = True,
        bind: bool = True,
        versioned: bool = True
    ) -> None:
        name, namespace = resolve_path(path)
        Entity.__init__(
            self, name, properties = [{}], namespace = namespace,
            create = create, bind = bind, versioned = versioned
        )

    def __getitem__(
        self,
        key: Any,
        /
    ) -> Any:
        key = self.encitemkey(key) if self.encitemkey else key
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
        return self.decitemval(result) if self.decitemval else result

    def get(
        self,
        key: Any,
        default: Any = None,
        /
    ) -> Any:
        key = self.encitemkey(key) if self.encitemkey else key
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
        return self.decitemval(result) if self.decitemval and result != default else result

    def setdefault(
        self,
        key: Any,
        default: Any = None,
        /
    ) -> Any:
        key = self.encitemkey(key) if self.encitemkey else key
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
                default = self.encitemval(default) if self.encitemval else default
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
        return default if not result else (self.decitemval(value) if self.decitemval else value)

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
            self.decitemkey(key) if self.decitemkey else key,
            self.decitemval(value) if self.decitemval else value
        )

    def pop(
        self,
        key: Any,
        default: Any = _unspecified_class(),
        /
    ) -> Any:
        key = self.encitemkey(key) if self.encitemkey else key
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
        return self.decitemval(result) if self.decitemval else result

    def __delitem__(
        self,
        key: Any,
        /
    ) -> None:
        key = self. encitemkey(key) if self.encitemkey else key
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
        key = self.encitemkey(key) if self.encitemkey else key
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
    ) -> None:
        key = self.encitemkey(key) if self.encitemkey else key
        value = self.encitemval(value) if self.encitemval else value
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
    ) -> None:
        dict_items = iter_items = None
        consumed = added = 0
        if args and isinstance(args[0], dict):
            dict_items = [
                (
                    self.encitemkey(key) if self.encitemkey else key,
                    self.encitemval(value) if self.encitemval else value
                )
                for key, value in args[0].items()
            ]
        elif args and isinstance(args[0], collections.abc.MutableMapping):
            iter_items = [
                (
                    self.encitemkey(key) if self.encitemkey else key,
                    self.encitemval(value) if self.encitemval else value
                )
                for key, value in args[0].items()
            ]
        elif args:
            iter_items = [
                (
                    self.encitemkey(key) if self.encitemkey else key,
                    self.encitemval(value) if self.encitemval else value
                )
                for key, value in args[0]
            ]
        kwargs_items = [
            (
                self.encitemkey(key) if self.encitemkey else key,
                self.encitemval(value) if self.encitemval else value
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

    __iter__: Callable[..., Generator[Union[Any, Tuple[Any, Any]], None, None]] = Missing()

    keys: Callable[..., Generator[Union[Any, Tuple[Any, Any]], None, None]] = Missing()

    values: Callable[..., Generator[Union[Any, Tuple[Any, Any]], None, None]] = Missing()

    items: Callable[..., Generator[Union[Any, Tuple[Any, Any]], None, None]] = Missing()
