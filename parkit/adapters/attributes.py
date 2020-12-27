import logging
import pickle
import types

from typing import (
    Any, ByteString, Callable, cast, Iterator, Optional
)

import parkit.storage.mixins as mixins

from parkit.adapters.missing import Missing
from parkit.storage.lmdbstate import LMDBState

logger = logging.getLogger(__name__)

class Attributes(LMDBState):

    @property
    def attributes(self) -> Iterator[str]:
        return self._attribute_keys()

    def _bind(
        self,
        dbindex: int,
        encode_key: Callable[..., ByteString] = Missing(),
        decode_key: Callable[..., Any] = Missing(),
        encode_value: Callable[..., ByteString] = Missing(),
        decode_value: Callable[..., Any] = Missing()
    ) -> None:
        setattr(self, '_attribute_keys', types.MethodType(mixins.iterator.iterate(
            dbindex,
            decode_key if not isinstance(decode_key, Missing) else \
            cast(Callable[..., Any], pickle.loads),
            decode_value if not isinstance(decode_value, Missing) else \
            cast(Callable[..., Any], pickle.loads),
            keys = True, values = False
        ), self))
        setattr(self, '_get_attribute', types.MethodType(mixins.dict.get(
            dbindex,
            encode_key if not isinstance(encode_key, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            decode_value if not isinstance(decode_value, Missing) else \
            cast(Callable[..., Any], pickle.loads)
        ), self))
        setattr(self, '_put_attribute', types.MethodType(mixins.dict.put(
            dbindex,
            encode_key if not isinstance(encode_key, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            encode_value if not isinstance(encode_value, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps)
        ), self))

    _attribute_keys: Callable[..., Iterator[str]] = Missing()

    _get_attribute: Callable[..., Any] = Missing()

    _put_attribute: Callable[..., None] = Missing()

class Attr:

    def __init__(
        self, readonly: bool = False
    ) -> None:
        self._readonly: bool = readonly
        self.public_name: Optional[str] = None
        self.private_name: Optional[str] = None

    def __set_name__(
        self,
        owner: str,
        name: str
    ) -> None:
        self.public_name = name
        self.private_name = '_' + name

    def __get__(
        self,
        obj: Attributes,
        objtype: type = None
    ) -> Any:
        value = obj._get_attribute(self.private_name)
        return value

    def __set__(
        self,
        obj: Attributes,
        value: Any
    ) -> None:
        if self._readonly:
            raise AttributeError()
        obj._put_attribute(self.private_name, value)
