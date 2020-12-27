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

    def __init__(
        self,
        encode_key: Callable[..., ByteString] = Missing(),
        decode_key: Callable[..., Any] = Missing(),
        encode_value: Callable[..., ByteString] = Missing(),
        decode_value: Callable[..., Any] = Missing()
    ):
        super().__init__()
        self._encattrkey: Callable[..., ByteString] = encode_key
        self._decattrkey: Callable[..., ByteString] = decode_key
        self._encattrval: Callable[..., ByteString] = encode_value
        self._decattrval: Callable[..., Any] = decode_value

    @property
    def attributes(self) -> Iterator[str]:
        return self._attribute_keys()

    def _bind(
        self, dbindex: int
    ) -> None:
        setattr(self, '_attribute_keys', types.MethodType(mixins.iterator.iterate(
            dbindex,
            self._decattrkey if not isinstance(self._decattrkey, Missing) else \
            cast(Callable[..., Any], pickle.loads),
            self._decattrval if not isinstance(self._decattrval, Missing) else \
            cast(Callable[..., Any], pickle.loads),
            keys = True, values = False
        ), self))
        setattr(self, '_get_attribute', types.MethodType(mixins.dict.get(
            dbindex,
            self._encattrkey if not isinstance(self._encattrkey, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            self._decattrval if not isinstance(self._decattrval, Missing) else \
            cast(Callable[..., Any], pickle.loads)
        ), self))
        setattr(self, '_put_attribute', types.MethodType(mixins.dict.put(
            dbindex,
            self._encattrkey if not isinstance(self._encattrkey, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            self._encattrval if not isinstance(self._encattrval, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps)
        ), self))
        del self.__dict__['_decattrkey']
        del self.__dict__['_encattrkey']
        del self.__dict__['_encattrval']
        del self.__dict__['_decattrval']

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
