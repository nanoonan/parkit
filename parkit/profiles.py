import logging

import parkit.constants as constants

from parkit.types import Profiles

logger = logging.getLogger(__name__)

_lmdb_profiles: Profiles = constants.LMDB_PROFILES

def get_lmdb_profiles() -> Profiles:
    return _lmdb_profiles
