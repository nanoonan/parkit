import logging
import pickle

from typing import (
    Any, Optional
)

import parkit.constants as constants

from parkit.utility import getenv

logger = logging.getLogger(__name__)

def self() -> Optional[Any]:
    try:
        return pickle.loads(getenv(constants.SELF_ENVNAME, str).encode())
    except ValueError:
        return None
