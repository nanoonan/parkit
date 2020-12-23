import glob
import logging
import orjson
import os
import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.storage.context import context
from parkit.storage.lmdbbase import LMDBBase
from parkit.storage.lmdbenv import get_environment
from parkit.utility import (
  create_class,
  getenv,
  resolve
)

from typing import (
  Any, Dict, Iterator, Tuple
)

logger = logging.getLogger(__name__)
  
def stats(namespace: str) -> Any:
  pass

def namespaces() -> Iterator[str]:
  for folder, subs, files in os.walk(getenv(constants.INSTALL_PATH_ENVNAME)):
    if folder != getenv(constants.INSTALL_PATH_ENVNAME) and \
    not folder.startswith(os.path.join(getenv(constants.INSTALL_PATH_ENVNAME), constants.PROCESS_NAMESPACE)) and \
    not folder.startswith(os.path.join(getenv(constants.INSTALL_PATH_ENVNAME), constants.LOCK_NAMESPACE)):
      yield '/'.join(folder[len(getenv(constants.INSTALL_PATH_ENVNAME)):].split(os.path.sep)[1:])

def objects(namespace: str = None) -> Iterator[Tuple[str, Dict[str, Any]]]:
  namespace = resolve(namespace, path = False) if namespace else constants.DEFAULT_NAMESPACE
  lmdb, metadata_db, _, instance_db = get_environment(namespace)
  with context(lmdb, write = False, inherit = True):
    cursor = thread.local.cursors[id(instance_db)]
    if cursor.first():
      while True:
        name = bytes(cursor.key()).decode('utf-8')
        obj_uuid = cursor.value()
        metadata = orjson.loads(bytes(thread.local.transaction.get(key = obj_uuid, db = metadata_db)))
        yield ('/'.join([namespace, name]), metadata)
        if not cursor.next():
          return

    
