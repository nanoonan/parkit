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

class DequeMeta(ObjectMeta):

    def __initialize_class__(cls):
        super().__initialize_class__()
        if isinstance(cls.pop, Missing):
            setattr(cls, 'pop', mixins.deque.pop(
                0, 1, False, cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads)
            ))
        if isinstance(cls.popleft, Missing):
            setattr(cls, 'popleft', mixins.deque.pop(
                0, 1, True, cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads)
            ))
        if isinstance(cls.append, Missing):
            setattr(cls, 'append', mixins.deque.append(
                0, 1, False, cls.encode_value if not isinstance(cls.encode_value, Missing) else \
                cast(Callable[..., Any], pickle.dumps)
            ))
        if isinstance(cls.appendleft, Missing):
            setattr(cls, 'appendleft', mixins.deque.append(
                0, 1, True, cls.encode_value if not isinstance(cls.encode_value, Missing) else \
                cast(Callable[..., Any], pickle.dumps)
            ))
        if isinstance(cls.__len__, Missing):
            setattr(cls, '__len__', mixins.collection.size(0, 1))
        if isinstance(cls.qsize, Missing):
            setattr(cls, 'qsize', mixins.collection.size(0, 1))
        if isinstance(cls.clear, Missing):
            setattr(cls, 'clear', mixins.collection.clear(0, 1))

    def __call__(cls, *args, **kwargs):
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)

class Deque(Object, metaclass = DequeMeta):

    def __init__(
        self, path: str, create: bool = True, bind: bool = True
    ) -> None:
        name, namespace = resolve(path, path = True)
        super().__init__(
            name, properties = [{'integerkey':True}, {'integerkey':True}],
            namespace = namespace, create = create, bind = bind
        )

    pop: Callable[..., Any] = Missing()

    popleft: Callable[..., Any] = Missing()

    append: Callable[..., None] = Missing()

    appendleft: Callable[..., None] = Missing()

    __len__: Callable[..., int] = Missing()

    qsize: Callable[..., int] = Missing()

    clear: Callable[..., None] = Missing()

    decode_value: Callable[..., Any] = Missing()

    encode_value: Callable[..., ByteString] = Missing()
