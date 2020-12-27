# pylint: disable = protected-access
import logging

from typing import (
    Any, Callable, Optional, Tuple, Union
)

import parkit.storage.threadlocal as thread

from parkit.storage.context import context
from parkit.storage.lmdbapi import LMDBAPI

logger = logging.getLogger(__name__)

def iterate(
    db0: int,
    decode_key: Callable[..., Any],
    decode_value: Callable[..., Any],
    keys: bool = True,
    values: bool = False
) -> Callable[..., Union[Any, Tuple[Any, Any]]]:

    def _iterate(
        self: LMDBAPI,
        transaction: bool = False,
        isolated: bool = False,
        zerocopy: bool = False,
        decode_key: Optional[Callable[..., Any]] = decode_key,
        decode_value: Optional[Callable[..., Any]] = decode_value
    ) -> Union[Any, Tuple[Any, Any]]:
        decode_key = lambda key: key if not decode_key else decode_key
        decode_value = lambda value: value if not decode_value else decode_value
        isolated = False if not transaction else isolated
        with context(
            self._environment, write = transaction, inherit = not isolated, buffers = zerocopy
        ):
            cursor = thread.local.cursors[db0]
            if not cursor.first():
                return
            while True:
                if keys and not values:
                    yield decode_key(cursor.key())
                elif values and not keys:
                    yield decode_value(cursor.value())
                else:
                    yield (decode_key(cursor.key()), decode_value(cursor.value()))
                if not cursor.next():
                    return

    return _iterate
