import datetime

from typing import (
  List, Tuple, TypedDict
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

Descriptor = TypedDict(
    'Descriptor', {
        'databases': List[Tuple[str, LMDBProperties]],
        'versioned': bool,
        'created': datetime.datetime,
        'type': str
    }
)
