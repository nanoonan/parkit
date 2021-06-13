import logging
import time
import uuid

from typing import (
    Any, Dict, Optional
)

import psutil

import parkit.constants as constants

from parkit.adapters.arguments import Arguments
from parkit.adapters.object import Object
from parkit.adapters.queue import Queue
from parkit.node import terminate_node
from parkit.storage.context import transaction_context

logger = logging.getLogger(__name__)

class AsyncExecution(Object):

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        task: Optional[Object] = None,
        arguments: Optional[Arguments] = None,
        metadata: Optional[Dict[str, Any]] = None,
        site: Optional[str] = None,
    ):
        def on_init(create: bool):
            if create:
                if task is None or arguments is None:
                    raise ValueError()
                with transaction_context(self._Entity__env, write = True) as (txn, _, _):
                    self.__status = 'submitted'
                    self.__result = None
                    self.__error = None
                    self.__start_timestamp = time.time_ns()
                    self.__end_timestamp = None
                    self.__pid = None
                    self.__node_uid = None
                    self.__task = task
                    self.__arguments = arguments
                    key = ':'.join([task.uuid, self.name])
                    assert txn.put(key = key.encode('utf-8'), value = b'', append = False)
                    queue = Queue(constants.EXECUTION_QUEUE_PATH)
                    queue.put(self)

        if path is None:
            path = '/'.join([constants.EXECUTION_NAMESPACE, str(uuid.uuid4())])

        super().__init__(path, metadata = metadata, site = site, on_init = on_init)

    @property
    def done(self) -> bool:
        return self.status in ['finished', 'failed', 'crashed']

    @property
    def result(self) -> Optional[Any]:
        return self.__result

    @property
    def error(self) -> Optional[Any]:
        return self.__error

    @property
    def status(self) -> str:
        with transaction_context(self._Entity__env, write = False):
            if self.__status == 'running':
                running = False
                try:
                    if psutil.pid_exists(self.__pid):
                        proc = psutil.Process(self.__pid)
                        env = proc.environ()
                        if constants.NODE_UID_ENVNAME in env:
                            if env[constants.NODE_UID_ENVNAME] == self.__node_uid:
                                running = proc.is_running()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                return 'crashed' if not running else self.__status
            return self.__status

    def cancel(self):
        if self.__status not in ['running', 'submitted']:
            return
        terminate = False
        with transaction_context(self._Entity__env, write = True):
            if self.__status == 'submitted':
                self.__status = 'cancelled'
            elif self.__status == 'running':
                terminate = True
        if terminate:
            terminate_node(
                self.__node_uid,
                self.site_uuid
            )
