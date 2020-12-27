import logging

from typing import (
    Any, ByteString, Callable
)

from parkit.adapters.attributes import Attributes
from parkit.adapters.metadata import Metadata
from parkit.adapters.missing import Missing
from parkit.storage import LMDBObject
from parkit.utility import resolve

logger = logging.getLogger(__name__)

ATTRS_INDEX = 0

class Object(LMDBObject, Attributes, Metadata):

    def __init__(
        self,
        path: str,
        create: bool = True,
        bind: bool = True,
        versioned: bool = False
    ) -> None:
        name, namespace = resolve(path, path = True)
        LMDBObject.__init__(
            self, name, properties = [{}], namespace = namespace,
            create = create, bind = bind, versioned = versioned
        )
        Attributes.__init__(
            self,
            encode_key = self.encode_key,
            decode_key = self.decode_key,
            encode_value = self.encode_value,
            decode_value = self.decode_value
        )
        Metadata.__init__(
            self,
            encode_key = self.encode_key,
            encode_value = self.encode_value,
            decode_value = self.decode_value
        )

    def _bind(self, *_: Any) -> None:
        Attributes._bind(self, ATTRS_INDEX)
        Metadata._bind(self)

    encode_key: Callable[..., ByteString] = Missing()

    decode_key: Callable[..., Any] = Missing()

    decode_value: Callable[..., Any] = Missing()

    encode_value: Callable[..., ByteString] = Missing()
