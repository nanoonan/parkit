# pylint: disable = too-few-public-methods, broad-except, protected-access
import collections
import os
import threading
import logging

from typing import (
    Any, Dict, Optional, Protocol, Set, Tuple
)

import lmdb

import parkit.constants as constants

from parkit.exceptions import (
    SiteNotFoundError,
    TransactionError
)

from parkit.storage.database import get_database_threadsafe
from parkit.storage.environment import get_environment_threadsafe

logger = logging.getLogger(__name__)

class StoragePath():

    sites: Dict[str, str] = {}

    def __init__(self, /, *, path: Optional[str] = None, site_uuid: Optional[str] = None):
        assert path or site_uuid
        if path:
            path = os.path.abspath(path)
            self._path = path
            if self._path not in StoragePath.sites:
                site_uuid, _, _, _, _, _ = get_environment_threadsafe(
                    self._path, constants.ROOT_NAMESPACE
                )
                StoragePath.sites[self._path] = site_uuid
                StoragePath.sites[site_uuid] = self._path
            self._uuid = StoragePath.sites[self._path]
        else:
            assert site_uuid
            self._uuid = site_uuid
            if self._uuid not in StoragePath.sites:
                raise SiteNotFoundError()
            self._path = StoragePath.sites[self._uuid]

    @property
    def path(self) -> str:
        return self._path

    @property
    def site_uuid(self) -> str:
        return self._uuid

class CursorDict(Protocol):

    def __getitem__(self, database: Any) -> lmdb.Cursor:
        """Get a cursor."""

class ExplicitCursorDict(dict):

    def __init__(self, txn: lmdb.Transaction):
        super().__init__()
        self._txn = txn

    def __getitem__(self, database: Any) -> lmdb.Cursor:
        key = id(database)
        if not dict.__contains__(self, key):
            database = get_database_threadsafe(key)
            cursor = self._txn.cursor(db = database)
            dict.__setitem__(self, key, cursor)
            return cursor
        return dict.__getitem__(self, key)

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
        buffers: bool = True,
        internal: bool = False,
    ) -> Tuple[lmdb.Transaction, CursorDict, Set[Any], bool]:
        try:
            if not self.stacks[env] or \
            write and not self.stacks[env][-1].write or \
            internal and self.stacks[env][-1].iterator:
                txn = env.begin(
                    write = write, buffers = buffers, parent = None
                )
                return (txn, ImplicitCursorDict(txn), set(), True)
            return (
                self.stacks[env][-1].transaction,
                self.stacks[env][-1].cursors,
                self.stacks[env][-1].changed,
                False
            )
        except lmdb.Error as exc:
            logger.exception('error while getting transaction')
            raise TransactionError() from exc

    def push(
        self,
        env: lmdb.Environment,
        write: bool = False,
        buffers: bool = True,
        iterator: bool = False
    ):
        try:
            txn = env.begin(
                write = write, buffers = buffers,
                parent = self.stacks[env][-1].transaction \
                if self.stacks[env] and self.stacks[env][-1].write else None
            )
            self.stacks[env].append(ExplicitContext(txn, write, iterator))
        except lmdb.Error as exc:
            logger.exception('error while popping transaction context')
            raise TransactionError() from exc

    def pop(
        self,
        env: lmdb.Environment,
        error: Optional[BaseException] = None
    ):
        try:
            context = self.stacks[env][-1]
            if not error:
                if context.write:
                    for obj in context.changed:
                        obj._Entity__increment_version(context.cursors)
                    for cursor in context.cursors.values():
                        cursor.close()
                    context.transaction.commit()
            else:
                for cursor in context.cursors.values():
                    cursor.close()
                context.transaction.abort()
        except BaseException as exc:
            logger.exception('error while popping transaction context')
            context.transaction.abort()
            raise exc from error
        finally:
            self.stacks[env].pop()

class ThreadLocalVars(threading.local):

    def __init__(self):

        super().__init__()

        self.context: ContextStacks = ContextStacks()

        self.storage_path: Optional[StoragePath] = None

local = ThreadLocalVars()
