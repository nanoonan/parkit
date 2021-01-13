import datetime

from typing import (
  Any, Dict, List, Tuple, TypedDict
)

LMDBProperties = TypedDict(
    'LMDBProperties', {
        'integerkey': bool,
        'dupsort': bool,
        'dupfixed': bool,
        'reverse_key': bool,
        'integerdup': bool
    },
    total = False
)

Profile = TypedDict(
    'Profile', {
        'LMDB_WRITE_MAP': bool,
        'LMDB_METASYNC': bool,
        'LMDB_MAP_ASYNC': bool,
        'LMDB_MAX_DBS': int,
        'LMDB_READONLY': bool,
        'LMDB_SYNC': bool,
        'LMDB_READAHEAD': bool,
        'LMDB_MEMINIT': bool,
        'LMDB_MAX_SPARE_TXNS': int,
        'LMDB_MAX_READERS': int
    }
)

Profiles = TypedDict(
    'Profiles', {
        'volatile': Profile,
        'persistent': Profile
    }
)

Descriptor = TypedDict(
    'Descriptor', {
        'databases': List[Tuple[str, LMDBProperties]],
        'versioned': bool,
        'created': datetime.datetime,
        'type': str,
        'custom': Dict[str, Any]
    }
)
