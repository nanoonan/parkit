# pylint: disable = broad-except
import logging

import parkit.storage.threadlocal as thread

from parkit.adapters.object import Object

logger = logging.getLogger(__name__)

class Sized(Object):

    def clear(self) -> None:
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin(write = True)
            for database in self._Entity__userdb:
                txn.drop(database, delete = False)
            if implicit:
                if self._Entity__vers:
                    self.increment_version(use_transaction = txn)
                txn.commit()
            elif self._Entity__vers:
                thread.local.changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)

    def __len__(self) -> int:
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin()
            result = sum([txn.stat(database)['entries'] for database in self._Entity__userdb])
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)
        return result
