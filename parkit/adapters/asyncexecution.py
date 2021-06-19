#
# reviewed: 6/14/21
#
import logging
import time
import uuid

from typing import (
    Any, Dict, Optional, Tuple
)

import parkit.constants as constants

from parkit.adapters.object import Object
from parkit.adapters.queue import Queue
from parkit.node import (
    is_running,
    terminate_node
)
from parkit.storage.context import transaction_context

logger = logging.getLogger(__name__)

class AsyncExecution(Object):

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        task: Optional[Object] = None,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        site_uuid: Optional[str] = None,
        create: bool = True,
        bind: bool = True
    ):
        self._status: str
        self._result: Any
        self._error: Any
        self._start_timestamp: int
        self._end_timestamp: Optional[int]
        self._pid: Optional[int]
        self._node_uid: Optional[str]
        self._task: Object
        self._task_uuid: str
        self._args: Tuple[Any, ...]
        self._kwargs: Dict[str, Any]

        def on_init(created: bool):
            if created:
                if task is None:
                    raise ValueError()
                with transaction_context(self._env, write = True) as (txn, _, _):
                    self._status = 'submitted'
                    self._result = None
                    self._error = None
                    self._start_timestamp = time.time_ns()
                    self._end_timestamp = None
                    self._pid = None
                    self._node_uid = None
                    self._task = task
                    self._task_uuid = task.uuid
                    self._args = args if args is not None else ()
                    self._kwargs = kwargs if kwargs is not None else {}
                    key1 = ':'.join([task.uuid, self.name])
                    key2 = ':'.join([self.name, task.uuid])
                    assert txn.put(key = key1.encode('utf-8'), value = b'', append = False)
                    assert txn.put(key = key2.encode('utf-8'), value = b'', append = False)
                    submit_queue = Queue(constants.SUBMIT_QUEUE_PATH)
                    submit_queue.put(self)

        if path is None:
            path = '/'.join([
                constants.EXECUTION_NAMESPACE,
                str(uuid.uuid4())
            ])

        super().__init__(
            path, metadata = metadata, site_uuid = site_uuid, on_init = on_init,
            create = create, bind = bind
        )

    @property
    def task(self) -> Object:
        return self._task

    @property
    def args(self) -> Tuple[Any, ...]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs

    @property
    def running(self) -> bool:
        return self.status == 'running'

    @property
    def done(self) -> bool:
        return self.status in ['finished', 'failed', 'crashed']

    @property
    def result(self) -> Optional[Any]:
        return self._result

    @property
    def error(self) -> Optional[Any]:
        return self._error

    @property
    def status(self) -> str:
        with transaction_context(self._env, write = False):
            status = self._status
            if status == 'running':
                node_uid = self._node_uid
                pid = self._pid
                assert node_uid is not None and pid is not None
                return 'crashed' \
                if not is_running(node_uid, pid) else 'running'
            return status

    def drop(self):
        node_uid = pid = None
        with transaction_context(self._env, write = True) as (txn, _, _):
            if self._status == 'running':
                node_uid = self._node_uid
                pid = self._pid
                assert node_uid is not None and pid is not None
            cursor = txn.cursor()
            assert cursor.set_range(self.name.encode('utf-8'))
            key_bytes = cursor.key()
            key_bytes = bytes(key_bytes) if isinstance(key_bytes, memoryview) else key_bytes
            key = key_bytes.decode('utf-8')
            assert key.startswith(self.name) and ':' in key
            task_uuid = key.split(':')[1]
            assert txn.delete(key = ':'.join([task_uuid, self.name]).encode('utf-8'))
            assert txn.delete(key = ':'.join([self.name, task_uuid]).encode('utf-8'))
            super().drop()
        if node_uid is not None:
            terminate_node(
                node_uid,
                pid
            )

    def cancel(self):
        if self._status not in ['running', 'submitted']:
            return
        node_uid = pid = None
        with transaction_context(self._env, write = True):
            status = self._status
            if status in ['submitted', 'running']:
                self._status = 'cancelled'
            if status == 'running':
                node_uid = self._node_uid
                pid = self._pid
                assert node_uid is not None and pid is not None
        if node_uid is not None:
            terminate_node(
                node_uid,
                pid
            )
