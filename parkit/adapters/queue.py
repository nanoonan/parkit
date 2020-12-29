import logging
import pickle

from typing import (
    Any, ByteString, Callable, cast
)

import parkit.storage.mixins as mixins

from parkit.storage import (
    Missing,
    Object,
    ObjectMeta
)
from parkit.utility import resolve

logger = logging.getLogger(__name__)

class QueueMeta(ObjectMeta):

    def __initialize_class__(cls):
        super().__initialize_class__()
        if isinstance(cls.get, Missing):
            setattr(cls, 'get', mixins.queue.get(
                0, True, cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads)
            ))
        if isinstance(cls.put, Missing):
            setattr(cls, 'put', mixins.queue.put(
                0, cls.encode_value if not isinstance(cls.encode_value, Missing) else \
                cast(Callable[..., Any], pickle.dumps)
            ))
        if isinstance(cls.__len__, Missing):
            setattr(cls, '__len__', mixins.collection.size(0))
        if isinstance(cls.qsize, Missing):
            setattr(cls, 'qsize', mixins.collection.size(0))
        if isinstance(cls.clear, Missing):
            setattr(cls, 'clear', mixins.collection.clear(0))

    def __call__(cls, *args, **kwargs):
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)

class Queue(Object, metaclass = QueueMeta):

    def __init__(
        self, path: str, create: bool = True, bind: bool = True
    ) -> None:
        name, namespace = resolve(path, path = True)
        super().__init__(
            name, properties = [{'integerkey':True}], namespace = namespace,
            create = create, bind = bind
        )

    get: Callable[..., Any] = Missing()

    put: Callable[..., None] = Missing()

    __len__: Callable[..., int] = Missing()

    clear: Callable[..., None] = Missing()

    qsize: Callable[..., int] = Missing()

    decode_value: Callable[..., Any] = Missing()

    encode_value: Callable[..., ByteString] = Missing()
