# pylint: disable = too-few-public-methods
import collections
import threading
import logging

from typing import (
    Any, Dict, List, Set, Tuple
)

import lmdb

logger = logging.getLogger(__name__)

class ThreadLocalVars(threading.local):

    def __init__(self):
        super().__init__()

        self.context: \
        List[Tuple[lmdb.Transaction, Dict[int, lmdb.Cursor], Set[Any], bool]] = []

        self.arguments: List[Tuple[bool, bool]] = []
        self.transaction: lmdb.Transaction = None
        self.changed: Set[Any] = set()
        self.cursors: Dict[int, lmdb.Cursor] = collections.defaultdict(lambda: None)

        self.storage_path: str = None

local = ThreadLocalVars()
