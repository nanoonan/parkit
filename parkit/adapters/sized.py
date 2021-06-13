# pylint: disable = broad-except
import logging

import parkit.storage.threadlocal as thread

from parkit.adapters.object import Object

logger = logging.getLogger(__name__)

class Sized(Object):

    def clear(self):
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True, internal = True)
            updated = False
            for database in self._Entity__userdb:
                if txn.stat(database)['entries']:
                    updated = True
                txn.drop(database, delete = False)
            if implicit:
                if updated and self._Entity__vers:
                    self._Entity__increment_version(cursors)
                txn.commit()
            elif updated and self._Entity__vers:
                changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)

    def __len__(self) -> int:
        try:
            txn, _, _, implicit = \
            thread.local.context.get(self._Entity__env, write = False, internal = True)
            result = txn.stat(self._Entity__userdb[0])['entries']
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        return result
