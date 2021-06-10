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
        'LMDB_INITIAL_MAP_SIZE': int,
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
        'default': Profile,
        'memory': Profile
    }
)

Descriptor = TypedDict(
    'Descriptor', {
        'databases': List[Tuple[str, LMDBProperties]],
        'uuid': str,
        'versioned': bool,
        'anonymous': bool,
        'created': str,
        'type': str,
        'origin': str,
        'metadata': Dict[str, Any]
    }
)
