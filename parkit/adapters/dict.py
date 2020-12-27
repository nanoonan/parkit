import logging
import pickle
import types

from typing import (
    Any, ByteString, Callable, cast, Iterator, Tuple
)

import parkit.storage.mixins as mixins

from parkit.adapters.metadata import Metadata
from parkit.adapters.missing import Missing
from parkit.storage import LMDBObject
from parkit.utility import resolve

logger = logging.getLogger(__name__)

DICT_INDEX = 0

class Dict(LMDBObject, Metadata):

    def __init__(
        self,
        path: str,
        create: bool = True,
        bind: bool = True,
        versioned: bool = False
    ) -> None:
        name, namespace = resolve(path, path = True)
        LMDBObject.__init__(
            self, name, properties = [{}, {}], namespace = namespace,
            create = create, bind = bind, versioned = versioned
        )
        Metadata.__init__(self)

    def _bind(self, *args: Any) -> None:
        Metadata._bind(self, self.encode_key, self.encode_value, self.decode_value)
        setattr(self, 'get', types.MethodType(mixins.dict.get(
            DICT_INDEX,
            self.encode_key if not isinstance(self.encode_key, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            self.decode_value if not isinstance(self.decode_value, Missing) else \
            cast(Callable[..., Any], pickle.loads)
        ), self))
        setattr(self, 'keys', types.MethodType(mixins.iterator.iterate(
            DICT_INDEX,
            self.decode_key if not isinstance(self.decode_key, Missing) else \
            cast(Callable[..., Any], pickle.loads),
            self.decode_value if not isinstance(self.decode_value, Missing) else \
            cast(Callable[..., Any], pickle.loads),
            keys = True, values = False
        ), self))
        setattr(self, 'values', types.MethodType(mixins.iterator.iterate(
            DICT_INDEX,
            self.decode_key if not isinstance(self.decode_key, Missing) else \
            cast(Callable[..., Any], pickle.loads),
            self.decode_value if not isinstance(self.decode_value, Missing) else \
            cast(Callable[..., Any], pickle.loads),
            keys = False, values = True
        ), self))
        setattr(self, 'items', types.MethodType(mixins.iterator.iterate(
            DICT_INDEX,
            self.decode_key if not isinstance(self.decode_key, Missing) else \
            cast(Callable[..., Any], pickle.loads),
            self.decode_value if not isinstance(self.decode_value, Missing) else \
            cast(Callable[..., Any], pickle.loads),
            keys = True, values = True
        ), self))
        setattr(self, 'pop', types.MethodType(mixins.dict.pop(
            DICT_INDEX,
            self.encode_key if not isinstance(self.encode_key, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            self.decode_value if not isinstance(self.decode_value, Missing) else \
            cast(Callable[..., Any], pickle.loads)
        ), self))
        setattr(self, 'popitem', types.MethodType(mixins.dict.popitem(
            DICT_INDEX,
            self.decode_key if not isinstance(self.decode_key, Missing) else \
            cast(Callable[..., Any], pickle.loads),
            self.decode_value if not isinstance(self.decode_value, Missing) else \
            cast(Callable[..., Any], pickle.loads)
        ), self))
        setattr(
            self, 'clear', types.MethodType(mixins.collection.clear(DICT_INDEX), self)
        )
        setattr(
            self, '_size', types.MethodType(mixins.collection.size(DICT_INDEX), self)
        )
        setattr(self, 'update', types.MethodType(mixins.dict.update(
            DICT_INDEX,
            self.encode_key if not isinstance(self.encode_key, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            self.encode_value if not isinstance(self.encode_value, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps)
        ), self))
        setattr(self, 'setdefault', types.MethodType(mixins.dict.setdefault(
            DICT_INDEX,
            self.encode_key if not isinstance(self.encode_key, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            self.encode_value if not isinstance(self.encode_value, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            self.decode_value if not isinstance(self.decode_value, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps)
        ), self))
        setattr(self, '_put', types.MethodType(mixins.dict.put(
            DICT_INDEX,
            self.encode_key if not isinstance(self.encode_key, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps),
            self.encode_value if not isinstance(self.encode_value, Missing) \
            else cast(Callable[..., ByteString], pickle.dumps)
        ), self))
        setattr(self, '_contains', types.MethodType(mixins.dict.contains(
            DICT_INDEX,
            self.encode_key if not isinstance(self.encode_key, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps)
        ), self))
        setattr(self, '_delete', types.MethodType(mixins.dict.delete(
            DICT_INDEX,
            self.encode_key if not isinstance(self.encode_key, Missing) else \
            cast(Callable[..., ByteString], pickle.dumps)
        ), self))

    def __getitem__(self, key: Any) -> Any:
        return self.get(key)

    def __setitem__(self, key: Any, value: Any) -> None:
        self._put(key, value)

    def __iter__(self) -> Iterator[Any]:
        return self.keys()

    def __len__(self) -> int:
        return self._size()

    def __contains__(self, key: Any) -> bool:
        return self._contains(key)

    def __delitem__(self, key: Any) -> None:
        self._delete(key)

    get: Callable[..., Any] = Missing()

    keys: Callable[..., Iterator[Any]] = Missing()

    values: Callable[..., Iterator[Any]] = Missing()

    items: Callable[..., Iterator[Tuple[Any, Any]]] = Missing()

    pop: Callable[..., Any] = Missing()

    popitem: Callable[..., Any] = Missing()

    clear: Callable[..., None] = Missing()

    update: Callable[..., None] = Missing()

    setdefault: Callable[..., Any] = Missing()

    _size: Callable[..., int] = Missing()

    _put: Callable[..., None] = Missing()

    _contains: Callable[..., bool] = Missing()

    _delete: Callable[..., None] = Missing()

    decode_key: Callable[..., Any] = Missing()

    encode_key: Callable[..., ByteString] = Missing()

    decode_value: Callable[..., Any] = Missing()

    encode_value: Callable[..., ByteString] = Missing()
