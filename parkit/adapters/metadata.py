import logging
import pickle
import types

from typing import (
    Any, ByteString, Callable, cast
)

import parkit.storage.mixins as mixins

from parkit.adapters.missing import Missing
from parkit.storage.lmdbstate import LMDBState

logger = logging.getLogger(__name__)

class Metadata(LMDBState):

    def __init__(
        self,
        encode_key: Callable[..., ByteString] = Missing(),
        encode_value: Callable[..., ByteString] = Missing(),
        decode_value: Callable[..., Any] = Missing()
    ):
        super().__init__()
        self._encmetakey: Callable[..., ByteString] = encode_key
        self._encmetaval: Callable[..., ByteString] = encode_value
        self._decmetaval: Callable[..., Any] = decode_value

    @property
    def metadata(self) -> Any:
        return self._get_metadata()

    @metadata.setter
    def metadata(self, value: Any) -> None:
        self._put_metadata(value)

    def _bind(self, *_: Any) -> None:
        setattr(self, '_get_metadata', types.MethodType(mixins.metadata.get(
            self._encmetakey if not isinstance(self._encmetakey, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            self._decmetaval if not isinstance(self._decmetaval, Missing) else \
            cast(Callable[..., Any], pickle.loads)
        ), self))
        setattr(self, '_put_metadata', types.MethodType(mixins.metadata.put(
            self._encmetakey if not isinstance(self._encmetakey, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            self._encmetaval if not isinstance(self._encmetaval, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps)
        ), self))
        del self.__dict__['_encmetakey']
        del self.__dict__['_encmetaval']
        del self.__dict__['_decmetaval']

    _get_metadata: Callable[..., Any] = Missing()

    _put_metadata: Callable[..., None] = Missing()
