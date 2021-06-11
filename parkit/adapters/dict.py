# pylint: disable = broad-except, not-callable, unused-import, no-self-use
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
from parkit.storage.context import transaction_context
from parkit.storage.entitymeta import (
    ClassBuilder,
    EntityMeta,
    Missing
)
from parkit.utility import compile_function

logger = logging.getLogger(__name__)

unspecified_class = types.new_class('__unspecified__')

def mkiter(
    keys: bool = True,
    values: bool = False
) -> Callable[..., Iterator[Union[Any, Tuple[Any, Any]]]]:
    code = """
def method(self) -> Iterator[Union[Any, Tuple[Any, Any]], None, None]:
    with transaction_context(self._Entity__env, write = False) as (_, cursors, _):
        data_cursor = cursors[self._Entity__userdb[0]]
        meta_cursor = cursors[self._Entity__userdb[1]] if self.get_metadata else None
        if not data_cursor.first():
            return
        if meta_cursor:
            meta_cursor.first()
        while True:
            {0}
            if not data_cursor.next():
                return
            if meta_cursor:
                meta_cursor.next()
"""
    if keys and not values:
        insert = """
            yield self.decode_key(data_cursor.key()) if self.decode_key else data_cursor.key()
        """.strip()
    elif values and not keys:
        insert = """
            yield \
            (self.decode_value(data_cursor.value(), pickle.loads(meta_cursor.value())) if self.get_metadata else self.decode_value(data_cursor.value())) \
            if self.decode_value else data_cursor.value()
        """
    else:
        insert = """
            yield (
                self.decode_key(data_cursor.key()) if self.decode_key else data_cursor.key(),
                (self.decode_value(data_cursor.value(), pickle.loads(meta_cursor.value())) if self.get_metadata else self.decode_value(data_cursor.value())) \
                if self.decode_value else data_cursor.value()
            )
        """
    return compile_function(
        code.format(insert), glbs = globals()
    )

class DictMeta(ClassBuilder):

    def __build_class__(cls, target, attr):
        if target == Dict:
            if attr == '__iter__':
                setattr(target, '__iter__', mkiter(keys = True, values = False))
            elif attr == 'keys':
                setattr(target, 'keys', mkiter(keys = True, values = False))
            elif attr == 'values':
                setattr(target, 'values', mkiter(keys = False, values = True))
            elif attr == 'items':
                setattr(target, 'items', mkiter(keys = True, values = True))

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
        metadata: Optional[typing.Dict[str, Any]] = None,
        site: Optional[str] = None,
        on_init: Optional[Callable[[bool], None]] = None
    ):
        super().__init__(
            path, db_properties = [{}, {}],
            create = create, bind = bind, on_init = on_init,
            metadata = metadata, site = site
        )

    def __getitem__(
        self,
        key: Any,
        /
    ) -> Any:
        key_bytes = self.encode_key(key) if self.encode_key else key
        try:
            txn, _, _, implicit = \
            thread.local.context.get(self._Entity__env, write = False)
            data = txn.get(key = key_bytes, db = self._Entity__userdb[0])
            if data is not None:
                meta = pickle.loads(txn.get(key = key_bytes, db = self._Entity__userdb[1])) \
                if self.get_metadata else None
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        if data is None:
            raise KeyError()
        return (self.decode_value(data, meta) if self.get_metadata else self.decode_value(data)) \
        if self.decode_value else data

    def get(
        self,
        key: Any,
        default: Any = None,
        /
    ) -> Any:
        key_bytes = self.encode_key(key) if self.encode_key else key
        try:
            txn, _, _, implicit = \
            thread.local.context.get(self._Entity__env, write = False)
            data = txn.get(key = key_bytes, db = self._Entity__userdb[0])
            if data is not None:
                meta = pickle.loads(txn.get(key = key_bytes, db = self._Entity__userdb[1])) \
                if self.get_metadata else None
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        if data is None:
            return default
        return (self.decode_value(data, meta) if self.get_metadata else self.decode_value(data)) \
        if self.decode_value else data

    def setdefault(
        self,
        key: Any,
        default: Any = None,
        /
    ) -> Any:
        key_bytes = self.encode_key(key) if self.encode_key else key
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True)
            cursor = cursors[self._Entity__userdb[0]]
            result = cursor.set_key(key_bytes)
            if result:
                data = cursor.value()
                meta = pickle.loads(txn.get(key = key_bytes, db = self._Entity__userdb[1])) \
                if self.get_metadata else None
            else:
                data = self.encode_value(default) if self.encode_value else default
                meta = pickle.dumps(self.get_metadata(default)) if self.get_metadata else None
                assert cursor.put(key = key_bytes, value = data, overwrite = True, append = False)
                if meta is not None:
                    assert txn.put(
                        key = key_bytes, value = meta, overwrite = True, append = False,
                        db = self._Entity__userdb[1]
                    )
                if implicit:
                    self._Entity__increment_version(cursors)
                else:
                    changed.add(self)
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        return default if not result else (
            (self.decode_value(data, meta) if self.get_metadata else self.decode_value(data)) \
            if self.decode_value else data
        )

    def popitem(self) -> Any:
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True)
            cursor = cursors[self._Entity__userdb[0]]
            result = cursor.last()
            if result:
                key = cursor.key()
                data = cursor.pop(key)
                meta = pickle.loads(txn.pop(key = key, db = self._Entity__userdb[1])) \
                if self.get_metadata else None
                if implicit:
                    self._Entity__increment_version(cursors)
                else:
                    changed.add(self)
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        if not result:
            raise KeyError()
        return (
            self.decode_key(key) if self.decode_key else key,
            (self.decode_value(data, meta) if self.get_metadata else self.decode_value(data)) \
            if self.decode_value else data
        )

    def pop(
        self,
        key: Any,
        default: Any = unspecified_class(),
        /
    ) -> Any:
        key_bytes = self.encode_key(key) if self.encode_key else key
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True)
            cursor = cursors[self._Entity__userdb[0]]
            data = cursor.pop(key_bytes)
            if data is not None:
                meta = pickle.loads(txn.pop(key = key_bytes, db = self._Entity__userdb[1])) \
                if self.get_metadata else None
            if implicit:
                if data is not None:
                    self._Entity__increment_version(cursors)
                txn.commit()
            elif data is not None:
                changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        if data is None and isinstance(default, unspecified_class):
            raise KeyError()
        if data is None:
            return default
        return (self.decode_value(data, meta) if self.get_metadata else self.decode_value(data)) \
        if self.decode_value else data

    def __delitem__(
        self,
        key: Any,
        /
    ):
        key_bytes = self. encode_key(key) if self.encode_key else key
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True)
            result = txn.delete(key = key_bytes, db = self._Entity__userdb[0])
            if result and self.get_metadata:
                assert txn.delete(key = key_bytes, db = self._Entity__userdb[1])
            if implicit:
                if result:
                    self._Entity__increment_version(cursors)
                txn.commit()
            elif result:
                changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        if not result:
            raise KeyError()

    def __contains__(
        self,
        key: Any,
        /
    ) -> bool:
        key_bytes = self.encode_key(key) if self.encode_key else key
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self._Entity__env, write = False)
            cursor = cursors[self._Entity__userdb[0]]
            result = cursor.set_key(key_bytes)
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        finally:
            if implicit and cursor:
                cursor.close()
        return result

    def __setitem__(
        self,
        key: Any,
        value: Any,
        /
    ):
        key_bytes = self.encode_key(key) if self.encode_key else key
        meta = pickle.dumps(self.get_metadata(value)) if self.get_metadata else None
        value_bytes = self.encode_value(value) if self.encode_value else value
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True)
            result = txn.put(
                key = key_bytes, value = value_bytes, overwrite = True, append = False,
                db = self._Entity__userdb[0]
            )
            if result and meta:
                assert txn.put(
                    key = key_bytes, value = meta, overwrite = True, append = False,
                    db = self._Entity__userdb[1]
            )
            if implicit:
                if result:
                    self._Entity__increment_version(cursors)
                txn.commit()
            elif result:
                changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)

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
                    (self.encode_value(value) if self.encode_value else value,
                    pickle.dumps(self.get_metadata(value)) if self.get_metadata else None)
                )
                for key, value in args[0].items()
            ]
        elif args and isinstance(args[0], collections.abc.MutableMapping):
            iter_items = [
                (
                    self.encode_key(key) if self.encode_key else key,
                    (self.encode_value(value) if self.encode_value else value,
                    pickle.dumps(self.get_metadata(value)) if self.get_metadata else None)
                )
                for key, value in args[0].items()
            ]
        elif args:
            iter_items = [
                (
                    self.encode_key(key) if self.encode_key else key,
                    (self.encode_value(value) if self.encode_value else value,
                    pickle.dumps(self.get_metadata(value)) if self.get_metadata else None)
                )
                for key, value in args[0]
            ]
        kwargs_items = [
            (
                self.encode_key(key) if self.encode_key else key,
                (self.encode_value(value) if self.encode_value else value,
                pickle.dumps(self.get_metadata(value)) if self.get_metadata else None)
            )
            for key, value in kwargs.items()
        ]
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True)
            cursor = cursors[self._Entity__userdb[0]]
            if dict_items:
                cons, add = cursor.putmulti([(key, value[0]) for key, value in dict_items])
                consumed += cons
                added += add
            if kwargs_items:
                cons, add = cursor.putmulti([(key, value[0]) for key, value in kwargs_items])
                consumed += cons
                added += add
            if iter_items:
                cons, add = cursor.putmulti([(key, value[0]) for key, value in iter_items])
                consumed += cons
                added += add
            if self.get_metadata:
                cursor = cursors[self._Entity__userdb[1]]
                if dict_items:
                    cursor.putmulti([(key, value[1]) for key, value in dict_items])
                if kwargs_items:
                    cursor.putmulti([(key, value[1]) for key, value in kwargs_items])
                if iter_items:
                    cursor.putmulti([(key, value[1]) for key, value in iter_items])
            if implicit:
                if added:
                    self._Entity__increment_version(cursors)
                txn.commit()
            elif added:
                changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)

    __iter__: Callable[..., Iterator[Union[Any, Tuple[Any, Any]]]] = Missing()

    keys: Callable[..., Iterator[Union[Any, Tuple[Any, Any]]]] = Missing()

    values: Callable[..., Iterator[Union[Any, Tuple[Any, Any]]]] = Missing()

    items: Callable[..., Iterator[Union[Any, Tuple[Any, Any]]]] = Missing()
