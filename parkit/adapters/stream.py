# pylint: disable = attribute-defined-outside-init
import logging
import queue

from typing import (
    Any, Callable, Dict, Optional
)

from parkit.adapters.queue import Queue
from parkit.storage.context import transaction_context
from parkit.storage.wait import wait

logger = logging.getLogger(__name__)

class Stream(Queue):

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        metadata: Optional[Dict[str, Any]] = None,
        maxsize: int = 0,
        site: Optional[str] = None,
        on_init: Optional[Callable[[bool], None]] = None
    ):
        def _on_init(create: bool):
            if create:
                self.__closed = False
            if on_init:
                on_init(create)

        super().__init__(
            path, on_init = _on_init,
            metadata = metadata, site = site, maxsize = maxsize
        )

    @property
    def closed(self):
        if not self.__closed:
            return False
        return self.__len__() == 0

    def close(self):
        if not self.__closed:
            with transaction_context(self._Entity__env, write = True) as (_, cursors, _):
                if not self.__closed:
                    self.__closed = True
                    self._Entity__increment_version(cursors)

    def __iter__(self):
        while True:
            if self.__closed and self.__len__() == 0:
                return
            with transaction_context(self._Entity__env, write = True):
                version = self.version
                success = False
                try:
                    item = super().get()
                    version += 1
                    success = True
                except queue.Empty:
                    pass
            if success:
                yield item
            wait(
                self,
                lambda: (self.version > version) or (self.__closed and self.__len__() == 0)
            )

    def put(
        self,
        item: Any,
        /
    ):
        with transaction_context(self._Entity__env, write = True):
            if self.__closed:
                raise ValueError()
            super().put(item)

    def get(self) -> Any:
        try:
            return super().get()
        except queue.Empty as exc:
            if self.__closed:
                raise ValueError() from exc
            raise exc
