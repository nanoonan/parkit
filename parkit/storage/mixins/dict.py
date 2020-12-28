# pylint: disable = broad-except, protected-access, too-few-public-methods
import collections.abc
import logging

from typing import (
    Any, ByteString, Callable, Dict, Iterable,
    MutableMapping, Optional, Tuple, Union
)

import parkit.storage.threadlocal as thread

from parkit.exceptions import abort

logger = logging.getLogger(__name__)

def get(
    db0: int,
    encode_key: Callable[..., ByteString],
    decode_value: Callable[..., Any]
) -> Callable[..., Any]:

    def _get(
        self,
        key: Any,
        default: Any = None,
        encode_key: Optional[Callable[..., ByteString]] = encode_key,
        decode_value: Optional[Callable[..., Any]] = decode_value
    ) -> Any:
        key = key if not encode_key else encode_key(key)
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin()
            result = txn.get(key = key, default = default, db = self._user_db[db0])
            if implicit:
                txn.commit()
        except BaseException as exc:
            if txn and implicit:
                txn.abort()
            abort(exc)
        return decode_value(result) if decode_value and result != default else result

    return _get

def setdefault(
    db0: int,
    encode_key:  Callable[..., ByteString],
    encode_value:  Callable[..., ByteString],
    decode_value:  Callable[..., Any]
) -> Callable[..., Any]:

    def _setdefault(
        self,
        key: Any,
        default: Any = None,
        encode_key:  Optional[Callable[..., ByteString]] = encode_key,
        encode_value:  Optional[Callable[..., ByteString]] = encode_value,
        decode_value:  Optional[Callable[..., Any]] = decode_value
    ) -> Any:
        key = key if not encode_key else encode_key(key)
        default = default if not encode_value else encode_value(default)
        try:
            txn = None
            cursor = thread.local.cursors[id(self._user_db[db0])]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[db0])
            result = cursor.set_key(key)
            if result:
                value = cursor.value()
            else:
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
        return default if not result else (decode_value(value) if decode_value else value)

    return _setdefault

def popitem(
    db0: int,
    decode_key: Callable[..., Any],
    decode_value: Callable[..., Any]
) -> Callable[..., Any]:

    def _popitem(
        self,
        decode_key: Optional[Callable[..., Any]] = decode_key,
        decode_value: Optional[Callable[..., Any]] = decode_value
    ) -> Any:
        try:
            txn = None
            cursor = thread.local.cursors[id(self._user_db[db0])]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[db0])
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
            decode_key(key) if decode_key else key,
            decode_value(value) if decode_value else value
        )

    return _popitem

def pop(
    db0: int,
    encode_key: Callable[..., ByteString],
    decode_value: Callable[..., Any]
) -> Callable[..., Any]:

    class Unspecified():
        pass

    def _pop(
        self,
        key: Any,
        default: Any = Unspecified(),
        encode_key: Optional[Callable[..., ByteString]] = encode_key,
        decode_value: Optional[Callable[..., Any]] = decode_value
    ) -> Any:
        key = key if not encode_key else encode_key(key)
        try:
            txn = None
            cursor = thread.local.cursors[id(self._user_db[db0])]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[db0])
            result = cursor.pop(key)
            if txn:
                if result is not None and self._versioned:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif not txn and result is not None and self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()
        if result is None and isinstance(default, Unspecified):
            raise KeyError()
        if result is None:
            return default
        return result if not decode_value else decode_value(result)

    return _pop

def delete(
    db0: int,
    encode_key: Callable[..., ByteString]
) -> Callable[..., None]:

    def _delete(
        self,
        key: Any,
        encode_key: Optional[Callable[..., ByteString]] = encode_key
    ) -> None:
        key = key if not encode_key else encode_key(key)
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            result = txn.delete(key = key, db = self._user_db[db0])
            if implicit:
                if result and self._versioned:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif not implicit and self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if txn and implicit:
                txn.abort()
            abort(exc)

    return _delete

def contains(
    db0: int,
    encode_key: Callable[..., ByteString]
) -> Callable[..., bool]:

    def _contains(
        self,
        key: Any,
        encode_key: Optional[Callable[..., ByteString]] = encode_key
    ) -> bool:
        key = key if not encode_key else encode_key(key)
        try:
            txn = None
            cursor = thread.local.cursors[id(self._user_db[db0])]
            if not cursor:
                txn = self._environment.begin()
                cursor = txn.cursor(db = self._user_db[db0])
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

    return _contains

def put(
    db0: int,
    encode_key: Callable[..., ByteString],
    encode_value: Callable[..., ByteString]
) -> Callable[..., None]:

    def _put(
        self,
        key: Any,
        value: Any,
        encode_key: Optional[Callable[..., ByteString]] = encode_key,
        encode_value: Optional[Callable[..., ByteString]] = encode_value
    ) -> None:
        key = key if not encode_key else encode_key(key)
        value = value if not encode_value else encode_value(value)
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            result = txn.put(
                key = key, value = value, overwrite = True, append = False,
                db = self._user_db[db0]
            )
            if implicit:
                if result and self._versioned:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif not implicit and result and self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if txn and implicit:
                txn.abort()
            abort(exc)

    return _put

def update(
    db0: int,
    encode_key: Callable[..., ByteString],
    encode_value: Callable[..., ByteString]
) -> Callable[..., None]:

    def _update(
        self,
        *args: Union[
            Tuple[()],
            Tuple[Union[Dict[Any, Any], MutableMapping[Any, Any], Iterable[Tuple[Any, Any]]]]
        ],
        encode_key: Optional[Callable[..., ByteString]] = encode_key,
        encode_value: Optional[Callable[..., ByteString]] = encode_value,
        **kwargs: Dict[Any, Any]
    ) -> None:
        dict_items = iter_items = None
        encode_key = lambda key: key if not encode_key else encode_key
        encode_value = lambda value: value if not encode_value else encode_value
        consumed = added = 0
        if args and isinstance(args[0], dict):
            dict_items = [
                (encode_key(key), encode_value(value))
                for key, value in args[0].items()
            ]
        elif args and isinstance(args[0], collections.abc.MutableMapping):
            iter_items = [
                (encode_key(key), encode_value(value))
                for key, value in args[0].items()
            ]
        elif args:
            iter_items = [
                (encode_key(key), encode_value(value))
                for key, value in args[0]
            ]
        kwargs_items = [
            (encode_key(key), encode_value(value))
            for key, value in kwargs.items()
        ]
        try:
            txn = None
            cursor = thread.local.cursors[id(self._user_db[db0])]
            if not cursor:
                txn = self._environment.begin(write = True)
                cursor = txn.cursor(db = self._user_db[db0])
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
            elif not txn and added and self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if txn:
                txn.abort()
            abort(exc)
        finally:
            if txn and cursor:
                cursor.close()

    return _update
