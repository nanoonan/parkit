# pylint: disable = broad-except
import logging

import parkit.storage.threadlocal as thread

from parkit.adapters.object import Object
from parkit.exceptions import abort

logger = logging.getLogger(__name__)

class Sized(Object):

    def clear(self) -> None:
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin(write = True)
            for database in self._user_db:
                txn.drop(database, delete = False)
            if implicit:
                if self._versioned:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif self._versioned:
                thread.local.changed.add(self)
        except BaseException as exc:
            if implicit and txn:
                txn.abort()
            abort(exc)

    def __len__(self) -> int:
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._environment.begin()
            result = sum([txn.stat(database)['entries'] for database in self._user_db])
            if implicit:
                txn.commit()
        except BaseException as exc:
            if implicit and txn:
                txn.abort()
            abort(exc)
        return result
