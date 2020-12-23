import contextlib
import logging
import parkit.constants as constants

from parkit.storage.lmdbenv import get_environment
from parkit.utility import create_string_digest

logger = logging.getLogger(__name__)

@contextlib.contextmanager
def lock(name):
	namespace = '/'.join([constants.LOCK_NAMESPACE, create_string_digest(name)])
	lmdb, _, _, _ = get_environment(namespace)
	try:
		txn = lmdb.begin(write = True)
		yield True
	finally:
		txn.commit()