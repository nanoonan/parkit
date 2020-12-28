# pylint: disable = too-few-public-methods
import logging

from typing import (
    Any, Callable, Optional
)

from parkit.storage import Missing

logger = logging.getLogger(__name__)

class Attributes():

    _get: Callable[..., Any] = Missing()

    _put: Callable[..., None] = Missing()

class Attr:

    def __init__(
        self, readonly: bool = False
    ) -> None:
        self._readonly: bool = readonly
        self.name: Optional[str] = None

    def __set_name__(
        self,
        owner: Attributes,
        name: str
    ) -> None:
        self.name = name

    def __get__(
        self,
        obj: Attributes,
        objtype: type = None
    ) -> Any:
        return obj._get(self.name)

    def __set__(
        self,
        obj: Attributes,
        value: Any
    ) -> None:
        if self._readonly:
            raise AttributeError()
        obj._put(self.name, value)
