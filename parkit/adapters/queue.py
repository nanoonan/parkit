import logging
import pickle
import types

from typing import (
    Any, ByteString, Callable, cast
)

import parkit.storage.mixins as mixins

from parkit.adapters.metadata import Metadata
from parkit.adapters.missing import Missing
from parkit.storage import LMDBObject
from parkit.utility import resolve

logger = logging.getLogger(__name__)

QUEUE_INDEX = 0

class Queue(LMDBObject, Metadata):

    def __init__(
        self, path: str, create: bool = True, bind: bool = True, versioned: bool = False
    ) -> None:
        name, namespace = resolve(path, path = True)
        LMDBObject.__init__(
            self, name, properties = [{'integerkey':True}], namespace = namespace,
            create = create, bind = bind, versioned = versioned,
        )
        Metadata.__init__(self)

    def __len__(self) -> int:
        return self.qsize()

    def _bind(self, *args: Any) -> None:
        Metadata._bind(self, self.encode_key, self.encode_value, self.decode_value)
        setattr(self, 'get', types.MethodType(mixins.queue.get(
            QUEUE_INDEX,
            self.decode_value if not isinstance(self.decode_value, Missing) else \
            cast(Callable[..., Any], pickle.loads)
        ), self))
        setattr(self, 'put', types.MethodType(mixins.queue.put(
            QUEUE_INDEX,
            self.encode_value if not isinstance(self.encode_value, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps)
        ), self))
        setattr(
            self, 'qsize', types.MethodType(mixins.collection.size(QUEUE_INDEX), self)
        )

    get: Callable[..., Any] = Missing()

    put: Callable[..., bool] = Missing()

    qsize: Callable[..., int] = Missing()

    encode_key: Callable[..., ByteString] = Missing()

    decode_value: Callable[..., Any] = Missing()

    encode_value: Callable[..., ByteString] = Missing()
