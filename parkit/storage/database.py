import logging
import threading

from typing import (
    Any, Dict, Optional, Union
)

import lmdb

from parkit.typeddicts import LMDBProperties

logger = logging.getLogger(__name__)

databases_lock: threading.Lock = threading.Lock()

databases: Dict[Union[int, str], Any] = {}

def get_database_threadsafe(key: Union[int, str]) -> Optional[Any]:
    try:
        with databases_lock:
            return databases[key]
    except KeyError:
        return None

def open_database_threadsafe(
    txn: lmdb.Transaction,
    env: lmdb.Environment,
    dbuid: str,
    properties: LMDBProperties,
    create: bool = False
) -> Any:
    with databases_lock:
        if dbuid not in databases:
            database = env.open_db(
                txn = txn, key = dbuid.encode('utf-8'),
                integerkey = properties['integerkey'] if 'integerkey' in properties else False,
                dupsort = properties['dupsort'] if 'dupsort' in properties else False,
                dupfixed = properties['dupfixed'] if 'dupfixed' in properties else False,
                integerdup = properties['integerdup'] if 'integerdup' in properties else False,
                reverse_key = properties['reverse_key'] if 'reverse_key' in properties else False,
                create = create
            )
            databases[id(database)] = database
            databases[dbuid] = database
    return databases[dbuid]
