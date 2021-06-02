import logging

import parkit.constants as constants

from parkit.typeddicts import Profiles

logger = logging.getLogger(__name__)

lmdb_profiles: Profiles = constants.LMDB_PROFILES

def get_lmdb_profiles() -> Profiles:
    return lmdb_profiles
