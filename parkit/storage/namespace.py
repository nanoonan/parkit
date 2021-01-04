import logging
import os

from typing import (
    Iterator, Optional
)

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.storage.context import context
from parkit.storage.environment import get_environment_threadsafe
from parkit.utility import (
    getenv,
    resolve_namespace
)

logger = logging.getLogger(__name__)

def namespaces() -> Iterator[str]:
    for folder, _, _ in os.walk(getenv(constants.INSTALL_PATH_ENVNAME)):
        if folder != getenv(constants.INSTALL_PATH_ENVNAME):
            yield '/'.join(
                folder[len(getenv(constants.INSTALL_PATH_ENVNAME)):].split(os.path.sep)[1:]
            )

def objects(namespace: Optional[str] = None) -> Iterator[str]:
    namespace = resolve_namespace(namespace) if namespace else constants.DEFAULT_NAMESPACE
    env, name_db, _, _, _ = get_environment_threadsafe(namespace)
    with context(env, write = False, inherit = True, buffers = False):
        cursor = thread.local.cursors[id(name_db)]
        if cursor.first():
            while True:
                result = cursor.key()
                name = bytes(result).decode('utf-8') if isinstance(result, memoryview) else \
                result.decode('utf-8')
                yield '/'.join([namespace, name]) if namespace else name
                if not cursor.next():
                    return
