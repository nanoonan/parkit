# pylint: disable = broad-except
import logging
import traceback

import parkit.storage.threadlocal as thread

from parkit.adapters.object import Object

logger = logging.getLogger(__name__)

class Sized(Object):

    def clear(self):
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._env, write = True, internal = True)
            updated = False
            for database in self._userdb:
                if txn.stat(database)['entries']:
                    updated = True
                txn.drop(database, delete = False)
            if implicit:
                if updated and self._versioned:
                    self._increment_version(cursors)
                txn.commit()
            elif updated and self._versioned:
                changed.add(self)
        except BaseException as exc:
            traceback.print_exc()
            self._abort(exc, txn, implicit)

    def __len__(self) -> int:
        try:
            txn, _, _, implicit = \
            thread.local.context.get(self._env, write = False, internal = True)
            result = txn.stat(self._userdb[0])['entries']
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._abort(exc, txn, implicit)
        return result
