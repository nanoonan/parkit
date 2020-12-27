import logging

import parkit.constants as constants

logger = logging.getLogger(__name__)

_lmdb_profiles = constants.lmdb_profiles

def get_lmdb_profiles():
    return _lmdb_profiles
