# pylint: disable = too-few-public-methods, broad-except
import importlib.util
import logging
import os

from typing import (
    Any, Dict, Optional
)

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from parkit.utility import create_string_digest

logger = logging.getLogger(__name__)

class FileObserver():

    class Observer(FileSystemEventHandler):

        def __init__(self):
            self._watches: Dict[str, Optional[str]] = {}

        def get_content_digest(self, path) -> Optional[str]:
            if not path in self._watches:
                with open(path, 'rt') as file:
                    contents = file.read()
                self._watches[path] = create_string_digest(contents)
            return self._watches[path]

        def on_created(self, event: Any):
            try:
                if event.src_path in self._watches:
                    if not event.is_directory:
                        with open(event.src_path, 'rt') as file:
                            contents = file.read()
                        self._watches[event.src_path] = create_string_digest(contents)
                    else:
                        self._watches[event.src_path] = None
            except Exception:
                logger.exception('file observer error')

        def on_deleted(self, event: Any):
            try:
                if event.src_path in self._watches:
                    self._watches[event.src_path] = None
            except Exception:
                logger.exception('file observer error')

        def on_modified(self, event: Any):
            try:
                if event.src_path in self._watches:
                    if not event.is_directory:
                        with open(event.src_path, 'rt') as file:
                            contents = file.read()
                        self._watches[event.src_path] = create_string_digest(contents)
                    else:
                        self._watches[event.src_path] = None
            except Exception:
                logger.exception('file observer error')

        def on_moved(self, event: Any):
            try:
                if event.src_path in self._watches:
                    self._watches[event.src_path] = None
                if event.dest_path in self._watches:
                    if not event.is_directory:
                        with open(event.dest_path, 'rt') as file:
                            contents = file.read()
                        self._watches[event.dest_path] = create_string_digest(contents)
                    else:
                        self._watches[event.dest_path] = None
            except Exception:
                logger.exception('file observer error')

    def __init__(self):
        try:
            self._observer = Observer()
            self._observer.start()
            self._watches = {}
        except Exception as exc:
            logger.exception('file observer error')
            raise exc

    def get_digest(
        self,
        module_name: str,
        function_name: str
    ) -> str:
        spec = importlib.util.find_spec(module_name)
        if spec is None:
            raise ModuleNotFoundError(module_name)
        filepath = spec.origin
        if filepath is None:
            raise ModuleNotFoundError(module_name)
        dirpath = os.path.abspath(os.path.dirname(filepath))
        if dirpath not in self._watches:
            handler = FileObserver.Observer()
            self._watches[dirpath] = (self._observer.schedule(handler, dirpath), handler)
        handler = self._watches[dirpath][1]
        content_digest = handler.get_content_digest(filepath)
        if content_digest is None:
            raise ModuleNotFoundError(module_name)
        return '-'.join([
            'module',
            ''.join([
                filepath, module_name, function_name,
                content_digest
            ])
        ])
