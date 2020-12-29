import logging
import pickle

from typing import (
    Any, ByteString, Callable, cast, Optional
)

import parkit.storage.mixins as mixins

from parkit.storage import (
    Missing,
    Object,
    ObjectMeta
)
from parkit.utility import resolve

logger = logging.getLogger(__name__)

class ShareableMeta(ObjectMeta):

    def __initialize_class__(cls):
        super().__initialize_class__()
        if isinstance(cls._get, Missing):
            setattr(cls, '_get', mixins.attributes.get(
                cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., Any], pickle.dumps),
                cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], pickle.loads)
            ))
        if isinstance(cls._put, Missing):
            setattr(cls, '_put', mixins.attributes.put(
                cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., Any], pickle.dumps),
                cls.encode_value if not isinstance(cls.encode_value, Missing) else \
                cast(Callable[..., Any], pickle.dumps)
            ))

    def __call__(cls, *args, **kwargs):
        cls.__initialize_class__()
        if not hasattr(cls, '__metaclass__'):
            setattr(cls, '__metaclass__', ShareableMeta)
        return super().__call__(*args, **kwargs)

class Shareable(Object, metaclass = ShareableMeta):

    def __init__(
        self,
        path: str,
        create: bool = True,
        bind: bool = True,
        versioned: bool = False,
        on_create: Callable[[], None] = lambda: None
    ) -> None:
        name, namespace = resolve(path, path = True)
        Object.__init__(
            self, name, properties = [], namespace = namespace,
            create = create, bind = bind, versioned = versioned,
            on_create = on_create
        )

    _get: Callable[..., Any] = Missing()

    _put: Callable[..., None] = Missing()

    encode_key: Callable[..., ByteString] = Missing()

    decode_value: Callable[..., Any] = Missing()

    encode_value: Callable[..., ByteString] = Missing()

class Attr:

    def __init__(
        self, readonly: bool = False
    ) -> None:
        self._readonly: bool = readonly
        self.name: Optional[str] = None

    def __set_name__(
        self,
        owner: Shareable,
        name: str
    ) -> None:
        self.name = name

    def __get__(
        self,
        obj: Shareable,
        objtype: type = None
    ) -> Any:
        return obj._get(self.name)

    def __set__(
        self,
        obj: Shareable,
        value: Any
    ) -> None:
        if self._readonly:
            raise AttributeError()
        obj._put(self.name, value)
