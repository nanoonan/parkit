# pylint: disable = broad-except, protected-access
import logging

from typing import Callable

import parkit.storage.threadlocal as thread

from parkit.exceptions import abort
from parkit.storage.lmdbapi import LMDBAPI

logger = logging.getLogger(__name__)

def clear(db0: int) -> Callable[..., None]:

    def _clear(self: LMDBAPI) -> None:
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            txn.drop(self._user_db[db0], delete = False)
            if implicit:
                if self._versioned:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if txn and implicit:
                txn.abort()
            abort(exc)

    return _clear

def size(db0: int) -> Callable[..., int]:

    def _size(self: LMDBAPI) -> int:
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin()
            result = txn.stat(self._user_db[db0])['entries']
            if implicit:
                txn.commit()
        except BaseException as exc:
            if txn and implicit:
                txn.abort()
            abort(exc)
        return result

    return _size
