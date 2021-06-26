# pylint: disable = too-few-public-methods, broad-except, protected-access
import collections
import threading
import logging

from typing import (
    Any, Optional, Protocol, Set, Tuple
)

import lmdb

from parkit.exceptions import TransactionError

from parkit.storage.database import get_database_threadsafe

logger = logging.getLogger(__name__)

class CursorDict(Protocol):

    def __getitem__(self, database: Any) -> lmdb.Cursor:
        """Get a cursor."""

class ExplicitCursorDict(dict):

    def __init__(self, txn: lmdb.Transaction):
        super().__init__()
        self._txn = txn

    def __getitem__(self, database: Any) -> lmdb.Cursor:
        try:
            key = id(database)
            if not dict.__contains__(self, key):
                database = get_database_threadsafe(key)
                cursor = self._txn.cursor(db = database)
                dict.__setitem__(self, key, cursor)
                return cursor
            return dict.__getitem__(self, key)
        except lmdb.Error as exc:
            raise TransactionError() from exc

class ImplicitCursorDict():

    def __init__(self, txn: lmdb.Transaction):
        self._txn = txn

    def __getitem__(self, database: Any) -> lmdb.Cursor:
        return self._txn.cursor(db = database)

class ExplicitContext():

    def __init__(
        self,
        transaction: lmdb.Transaction,
        write: bool,
        iterator: bool
    ):
        self.transaction = transaction
        self.write = write
        self.iterator = iterator
        self.changed: Set[Any] = set()
        self.cursors = ExplicitCursorDict(transaction)

    def __str__(self):
        return 'tx: {0} write: {1} iterator: {2}'.format(
            self.transaction, self.write, self.iterator
        )

class ContextStacks():

    def __init__(self):
        self.stacks = collections.defaultdict(lambda: [])

    def get(
        self,
        env: lmdb.Environment,
        /, *,
        write: bool = False,
        internal: bool = False,
    ) -> Tuple[lmdb.Transaction, CursorDict, Set[Any], bool]:
        try:
            if not self.stacks[env] or \
            write and not self.stacks[env][-1].write or \
            internal and self.stacks[env][-1].iterator:
                txn = env.begin(
                    write = write, buffers = True, parent = None
                )
                return (txn, ImplicitCursorDict(txn), set(), True)
            return (
                self.stacks[env][-1].transaction,
                self.stacks[env][-1].cursors,
                self.stacks[env][-1].changed,
                False
            )
        except lmdb.Error as exc:
            raise TransactionError() from exc

    def push(
        self,
        env: lmdb.Environment,
        write: bool = False,
        iterator: bool = False
    ):
        try:
            txn = env.begin(
                write = write, buffers = True,
                parent = self.stacks[env][-1].transaction \
                if self.stacks[env] and self.stacks[env][-1].write else None
            )
            self.stacks[env].append(ExplicitContext(txn, write, iterator))
        except lmdb.Error as exc:
            raise TransactionError() from exc

    def pop(
        self,
        env: lmdb.Environment,
        /, *,
        abort: bool = False
    ):
        try:
            try:
                context = self.stacks[env][-1]
                if not abort:
                    if context.write:
                        for obj in context.changed:
                            obj._increment_version(context.cursors)
                        for cursor in context.cursors.values():
                            cursor.close()
                        context.transaction.commit()
                else:
                    for cursor in context.cursors.values():
                        cursor.close()
                    context.transaction.abort()
            except BaseException as exc:
                context.transaction.abort()
                raise exc
            finally:
                self.stacks[env].pop()
        except lmdb.Error as exc:
            raise TransactionError from exc

class ThreadLocalVars(threading.local):

    def __init__(self):

        super().__init__()

        self.context: ContextStacks = ContextStacks()

        self.default_site: Optional[Tuple[str, str]] = None

local = ThreadLocalVars()
