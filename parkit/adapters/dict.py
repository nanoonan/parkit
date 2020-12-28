import logging
import pickle

from typing import (
    Any, ByteString, Callable, cast, Iterator, Tuple
)

import parkit.storage.mixins as mixins

from parkit.storage import (
    Missing,
    Object,
    ObjectMeta
)
from parkit.utility import resolve

logger = logging.getLogger(__name__)

class DictMeta(ObjectMeta):

    def __initialize_class__(cls):
        super().__initialize_class__()
        if isinstance(cls.get, Missing):
            setattr(cls, 'get', mixins.dict.get(
                0, cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., ByteString], pickle.dumps),
                cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads)
            ))
        if isinstance(cls.__getitem__, Missing):
            setattr(cls, '__getitem__', mixins.dict.get(
                0, cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., ByteString], pickle.dumps),
                cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads)
            ))
        if isinstance(cls.__iter__, Missing):
            setattr(cls, '__iter__', mixins.iterator.iterate(
                0, cls.decode_key if not isinstance(cls.decode_key, Missing) else \
                cast(Callable[..., Any], pickle.loads),
                cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads),
                keys = True, values = False
            ))
        if isinstance(cls.keys, Missing):
            setattr(cls, 'keys', mixins.iterator.iterate(
                0, cls.decode_key if not isinstance(cls.decode_key, Missing) else \
                cast(Callable[..., Any], pickle.loads),
                cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads),
                keys = True, values = False
            ))
        if isinstance(cls.values, Missing):
            setattr(cls, 'values', mixins.iterator.iterate(
                0, cls.decode_key if not isinstance(cls.decode_key, Missing) else \
                cast(Callable[..., Any], pickle.loads),
                cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads),
                keys = False, values = True
            ))
        if isinstance(cls.items, Missing):
            setattr(cls, 'items', mixins.iterator.iterate(
                0, cls.decode_key if not isinstance(cls.decode_key, Missing) else \
                cast(Callable[..., Any], pickle.loads),
                cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads),
                keys = True, values = True
            ))
        if isinstance(cls.pop, Missing):
            setattr(cls, 'pop', mixins.dict.pop(
                0, cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., ByteString], pickle.dumps),
                cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads)
            ))
        if isinstance(cls.popitem, Missing):
            setattr(cls, 'popitem', mixins.dict.popitem(
                0, cls.decode_key if not isinstance(cls.decode_key, Missing) else \
                cast(Callable[..., Any], pickle.loads),
                cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads)
            ))
        if isinstance(cls.clear, Missing):
            setattr(cls, 'clear', mixins.collection.clear(0))
        if isinstance(cls.__len__, Missing):
            setattr(cls, '__len__', mixins.collection.size(0))
        if isinstance(cls.update, Missing):
            setattr(cls, 'update', mixins.dict.update(
                0, cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., ByteString], pickle.dumps),
                cls.encode_value if not isinstance(cls.encode_value, Missing) else \
                cast(Callable[..., ByteString], pickle.dumps)
            ))
        if isinstance(cls.setdefault, Missing):
            setattr(cls, 'setdefault', mixins.dict.setdefault(
                0, cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., ByteString], pickle.dumps),
                cls.encode_value if not isinstance(cls.encode_value, Missing) else \
                cast(Callable[..., ByteString], pickle.dumps),
                cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., ByteString], pickle.dumps)
            ))
        if isinstance(cls.__setitem__, Missing):
            setattr(cls, '__setitem__', mixins.dict.put(
                0, cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., ByteString], pickle.dumps),
                cls.encode_value if not isinstance(cls.encode_value, Missing) \
                else cast(Callable[..., ByteString], pickle.dumps)
            ))
        if isinstance(cls.__contains__, Missing):
            setattr(cls, '_contains', mixins.dict.contains(
                0, cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., ByteString], pickle.dumps)
            ))
        if isinstance(cls.__delitem__, Missing):
            setattr(cls, '_delete', mixins.dict.delete(
                0, cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., ByteString], pickle.dumps)
            ))

    def __call__(cls, *args, **kwargs):
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)

class Dict(Object, metaclass = DictMeta):

    def __init__(
        self,
        path: str,
        create: bool = True,
        bind: bool = True,
        versioned: bool = False
    ) -> None:
        name, namespace = resolve(path, path = True)
        super().__init__(
            name, properties = [{}], namespace = namespace,
            create = create, bind = bind, versioned = versioned
        )

    __getitem__: Callable[..., Any] = Missing()

    __setitem__: Callable[..., None] = Missing()

    __iter__: Callable[..., Iterator[Any]] = Missing()

    __len__: Callable[..., int] = Missing()

    __contains__: Callable[..., bool] = Missing()

    __delitem__: Callable[..., None] = Missing()

    get: Callable[..., Any] = Missing()

    keys: Callable[..., Iterator[Any]] = Missing()

    values: Callable[..., Iterator[Any]] = Missing()

    items: Callable[..., Iterator[Tuple[Any, Any]]] = Missing()

    pop: Callable[..., Any] = Missing()

    popitem: Callable[..., Any] = Missing()

    clear: Callable[..., None] = Missing()

    update: Callable[..., None] = Missing()

    setdefault: Callable[..., Any] = Missing()

    decode_key: Callable[..., Any] = Missing()

    encode_key: Callable[..., ByteString] = Missing()

    decode_value: Callable[..., Any] = Missing()

    encode_value: Callable[..., ByteString] = Missing()
