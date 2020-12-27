# pylint: disable = too-many-instance-attributes, no-member, too-few-public-methods
from typing import (
    Dict, List, Union
)

import lmdb

from parkit.utility import get_memory_size

class LMDBState():

    def __init__(self) -> None:
        self._environment: lmdb.Environment
        self._encoded_name: bytes
        self._namespace: str
        self._name_db: lmdb._Database
        self._metadata_db: lmdb._Database
        self._version_db: lmdb._Database
        self._descriptor_db: lmdb._Database
        self._name_dbuid: int
        self._metadata_dbuid: int
        self._version_dbuid: int
        self._descriptor_dbuid: int
        self._user_db: List[lmdb._Database]
        self._user_dbuid: List[str]
        self._versioned: bool
        self._creator: bool
        self._uuid_bytes: bytes

    @property
    def debug(self) -> Dict[str, Union[int, str, bool]]:
        return dict(
            memory_size = get_memory_size(self)
        )
