#
# reviewed: 6/14/21
#
import logging
import pickle
import time
import uuid

from typing import (
    Any, ByteString, Callable, cast, Dict, Optional, Tuple
)

import cloudpickle

from cacheout.lru import LRUCache

import parkit.constants as constants

from parkit.adapters.object import Object
from parkit.adapters.queue import Queue
from parkit.node import (
    is_running,
    terminate_node
)
from parkit.storage.context import transaction_context
from parkit.utility import getenv

logger = logging.getLogger(__name__)

class Task(Object):

    _running_cache: LRUCache = LRUCache(
        maxsize = constants.RUNNING_CACHE_MAXSIZE,
        ttl = constants.RUNNING_CACHE_TTL
    )

    encode_attr_value: Optional[Callable[..., ByteString]] = \
    cast(Callable[..., ByteString], staticmethod(cloudpickle.dumps))

    decode_attr_value: Optional[Callable[..., Any]] = \
    cast(Callable[..., Any], staticmethod(cloudpickle.loads))

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        asyncable: Optional[Object] = None,
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
        self._created_timestamp: int
        self._start_timestamp: Optional[int]
        self._end_timestamp: Optional[int]
        self._pid: Optional[int]
        self._node_uid: Optional[str]
        self._asyncable: Object
        self._asyncable_uuid: str
        self._args: Tuple[Any, ...]
        self._kwargs: Dict[str, Any]

        def on_init(created: bool):
            if created:
                if asyncable is None:
                    raise ValueError()
                with transaction_context(self._env, write = True) as (txn, _, _):
                    self._status = 'submitted'
                    self._result = None
                    self._error = None
                    self._created_timestamp = time.time_ns()
                    self._start_timestamp = None
                    self._end_timestamp = None
                    self._pid = None
                    self._node_uid = None
                    self._asyncable = asyncable
                    self._asyncable_uuid = asyncable.uuid
                    self._args = args if args is not None else ()
                    self._kwargs = kwargs if kwargs is not None else {}
                    key1 = ':'.join([asyncable.uuid, self.name])
                    key2 = ':'.join([self.name, asyncable.uuid])
                    assert txn.put(key = key1.encode('utf-8'), value = b'', append = False)
                    assert txn.put(key = key2.encode('utf-8'), value = b'', append = False)
                    submit_queue = Queue(constants.SUBMIT_QUEUE_PATH, create = True)
                    submit_queue.put(self)

        if path is None:
            path = '/'.join([
                constants.TASK_NAMESPACE,
                str(uuid.uuid4())
            ])

        super().__init__(
            path, metadata = metadata, site_uuid = site_uuid, on_init = on_init,
            create = create, bind = bind
        )

    @property
    def asyncable(self) -> Object:
        return self._asyncable

    @property
    def created(self) -> int:
        return self._created_timestamp

    @property
    def start(self) -> Optional[int]:
        return self._start_timestamp

    @property
    def end(self) -> Optional[int]:
        return self._end_timestamp

    @property
    def args(self) -> Tuple[Any, ...]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs

    @property
    def pid(self) -> Optional[int]:
        return self._pid

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
                status = self._running_cache.get(
                    (node_uid, pid),
                    default = \
                    lambda key: 'running' if is_running(key[0], key[1]) else 'crashed'
                )
                if status == 'crashed':
                    self._running_cache.set(
                        (node_uid, pid),
                        'crashed',
                        ttl = 0
                    )
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
            asyncable_uuid = key.split(':')[1]
            assert txn.delete(key = ':'.join([asyncable_uuid, self.name]).encode('utf-8'))
            assert txn.delete(key = ':'.join([self.name, asyncable_uuid]).encode('utf-8'))
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

def task() -> Optional[Task]:
    try:
        return pickle.loads(getenv(constants.SELF_ENVNAME, str).encode())
    except ValueError:
        return None
