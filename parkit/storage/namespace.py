# pylint: disable = c-extension-no-member
import logging
import os

from typing import (
    Iterator, Optional, Tuple
)

import orjson

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.storage.context import context
from parkit.storage.environment import get_environment_threadsafe
from parkit.typeddicts import Descriptor
from parkit.utility import (
    getenv,
    resolve_namespace
)

logger = logging.getLogger(__name__)

def namespaces() -> Iterator[str]:
    """
    Namespaces
    """
    for folder, _, _ in os.walk(getenv(constants.STORAGE_PATH_ENVNAME)):
        if folder != getenv(constants.STORAGE_PATH_ENVNAME):
            top_level_namespace = \
            folder[len(getenv(constants.STORAGE_PATH_ENVNAME)):].split(os.path.sep)[1]
            if not top_level_namespace.startswith('__') and \
            not top_level_namespace.startswith('__'):
                yield '/'.join(
                    folder[len(getenv(constants.STORAGE_PATH_ENVNAME)):].split(os.path.sep)[1:]
                )

def objects(namespace: Optional[str] = None) -> Iterator[Tuple[str, Descriptor]]:
    """
    Objects
    """
    namespace = resolve_namespace(namespace) if namespace else constants.DEFAULT_NAMESPACE
    env, name_db, _, _, descriptor_db = get_environment_threadsafe(namespace)
    with context(env, write = False, inherit = True, buffers = False):
        cursor = thread.local.cursors[id(name_db)]
        if cursor.first():
            names = {}
            while True:
                result = cursor.key()
                name = bytes(result).decode('utf-8') if isinstance(result, memoryview) else \
                result.decode('utf-8')
                if not name.startswith('__') and not name.endswith('__'):
                    names[cursor.value()] = '/'.join([namespace, name]) if namespace else name
                if not cursor.next():
                    break
            cursor = thread.local.cursors[id(descriptor_db)]
            if cursor.first():
                while True:
                    if cursor.key() in names:
                        result = cursor.value()
                        descriptor = orjson.loads(bytes(result) \
                        if isinstance(result, memoryview) else result)
                        yield (
                            names[cursor.key()],
                            descriptor
                        )
                    if not cursor.next():
                        return
