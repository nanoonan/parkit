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

    @property
    def metadata(self) -> Any:
        return self._get_metadata()

    @metadata.setter
    def metadata(self, value: Any) -> None:
        self._put_metadata(value)

    def _bind(
        self,
        encode_key: Callable[..., ByteString] = Missing(),
        encode_value: Callable[..., ByteString] = Missing(),
        decode_value: Callable[..., Any] = Missing()
    ) -> None:
        setattr(self, '_get_metadata', types.MethodType(mixins.metadata.get(
            encode_key if not isinstance(encode_key, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            decode_value if not isinstance(decode_value, Missing) else \
            cast(Callable[..., Any], pickle.loads)
        ), self))
        setattr(self, '_put_metadata', types.MethodType(mixins.metadata.put(
            encode_key if not isinstance(encode_key, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            encode_value if not isinstance(encode_value, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps)
        ), self))

    _get_metadata: Callable[..., Any] = Missing()

    _put_metadata: Callable[..., None] = Missing()
