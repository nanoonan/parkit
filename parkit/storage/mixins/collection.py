# pylint: disable = broad-except, protected-access
import logging

from typing import Callable

import parkit.storage.threadlocal as thread

from parkit.exceptions import abort

logger = logging.getLogger(__name__)

def clear(*dbs: int) -> Callable[..., None]:

    def _clear(self) -> None:
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            for index in dbs:
                txn.drop(self._user_db[index], delete = False)
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

def size(*dbs: int) -> Callable[..., int]:

    def _size(self) -> int:
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin()
            result = sum([txn.stat(self._user_db[index])['entries'] for index in dbs])
            if implicit:
                txn.commit()
        except BaseException as exc:
            if txn and implicit:
                txn.abort()
            abort(exc)
        return result

    return _size
