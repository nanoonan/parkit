# pylint: disable = broad-except, no-value-for-parameter, using-constant-test
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

from parkit.adapters.sized import Sized
from parkit.exceptions import abort
from parkit.storage import (
    context,
    EntityMeta,
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
) -> Callable[..., Generator[Union[Any, Tuple[Any, Any]], None, None]]:
    code = """
def method(self) -> Generator[Union[Any, Tuple[Any, Any]], None, None]:
    with context(
        self._environment, write = False,
        inherit = True, buffers = True
    ):
        cursor = thread.local.cursors[id(self._user_db[0])]
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
    return compile_function(
        code, insert,
        globals_dict = dict(
            context = context,
            id = id,
            thread = thread
        )
    )

class DictMeta(EntityMeta):

    def __initialize_class__(cls):
        super().__initialize_class__()
        if isinstance(cls.__iter__, Missing):
            setattr(cls, '__iter__', mkiter(keys = True, values = False))
        if isinstance(cls.keys, Missing):
            setattr(cls, 'keys', mkiter(keys = True, values = False))
        if isinstance(cls.values, Missing):
            setattr(cls, 'values', mkiter(keys = False, values = True))
        if isinstance(cls.items, Missing):
            setattr(cls, 'items', mkiter(keys = True, values = True))

    def __call__(cls, *args, **kwargs):
        print('dict meta')
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)

class Dict(Sized, metaclass = DictMeta):

    decode_key: Callable[..., Any] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encode_key: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    decode_value: Callable[..., Any] = \
    cast(Callable[..., Any], staticmethod(pickle.loads))

    encode_value: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(pickle.dumps))

    def __init__(
        self,
        path: str,
        create: bool = True,
        bind: bool = True,
        versioned: bool = False,
        on_create = None
    ) -> None:
        name, namespace = resolve_path(path)
        super().__init__(
            name, properties = [{}], namespace = namespace,
            create = create, bind = bind, versioned = versioned,
            on_create = on_create
        )

    def __getitem__(
        self,
        key: Any,
        default: Any = None
    ) -> Any:
        key = self.encode_key(key) if self.encode_key else key
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin()
            result = txn.get(key = key, default = default, db = self._user_db[0])
            if implicit:
                txn.commit()
        except BaseException as exc:
            if implicit and txn:
                txn.abort()
            abort(exc)
        return self.decode_value(result) if self.decode_value and result != default else result

    get = __getitem__

    def setdefault(
        self,
        key: Any,
        default: Any = None
    ) -> Any:
        key = self.encode_key(key) if self.encode_key else key
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._user_db[0])]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[0])
            result = cursor.set_key(key)
            if result:
                value = cursor.value()
            else:
                default = self.encode_value(default) if self.encode_value else default
                assert cursor.put(key = key, value = default)
                if txn and self._versioned:
                    self.increment_version(use_transaction = txn)
                elif not txn and self._versioned:
                    thread.local.changed.add(self)
            if txn:
                txn.commit()
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()
        return default if not result else (self.decode_value(value) if self.decode_value else value)

    def popitem(self) -> Any:
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._user_db[0])]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[0])
            result = cursor.last()
            if result:
                key = cursor.key()
                value = cursor.pop(key)
                if txn and self._versioned:
                    self.increment_version(use_transaction = txn)
                elif not txn and self._versioned:
                    thread.local.changed.add(self)
            if txn:
                txn.commit()
            if not result:
                raise KeyError()
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
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
        default: Any = _unspecified_class()
    ) -> Any:
        key = self.encode_key(key) if self.encode_key else key
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._user_db[0])]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[0])
            result = cursor.pop(key)
            if txn:
                if result is not None and self._versioned:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif result is not None and self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
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
        key: Any
    ) -> None:
        key = self. encode_key(key) if self.encode_key else key
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            result = txn.delete(key = key, db = self._user_db[0])
            if implicit:
                if result and self._versioned:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if implicit and txn:
                txn.abort()
            abort(exc)

    def __contains__(
        self,
        key: Any
    ) -> bool:
        key = self.encode_key(key) if self.encode_key else key
        try:
            txn = cursor = None
            cursor = thread.local.cursors[id(self._user_db[0])]
            if not cursor:
                txn = self._environment.begin()
                cursor = txn.cursor(db = self._user_db[0])
            result = cursor.set_key(key)
            if txn:
                txn.commit()
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()
        return result

    def __setitem__(
        self,
        key: Any,
        value: Any
    ) -> None:
        key = self.encode_key(key) if self.encode_key else key
        value = self.encode_value(value) if self.encode_value else value
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            result = txn.put(
                key = key, value = value, overwrite = True, append = False,
                db = self._user_db[0]
            )
            if implicit:
                if result and self._versioned:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif result and self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if implicit and txn:
                txn.abort()
            abort(exc)

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
            cursor = thread.local.cursors[id(self._user_db[0])]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[0])
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
                if added and self._versioned:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif added and self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()

    __iter__: Callable[..., Generator[Union[Any, Tuple[Any, Any]], None, None]] = Missing()

    keys: Callable[..., Generator[Union[Any, Tuple[Any, Any]], None, None]] = Missing()

    values: Callable[..., Generator[Union[Any, Tuple[Any, Any]], None, None]] = Missing()

    items: Callable[..., Generator[Union[Any, Tuple[Any, Any]], None, None]] = Missing()
