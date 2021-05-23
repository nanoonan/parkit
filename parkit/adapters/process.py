# pylint: disable = broad-except, non-parent-init-called, too-many-public-methods, super-init-not-called, no-self-use, dangerous-default-value
import datetime
import logging
import typing

from typing import (
    Any, Callable, Iterator, List, Optional, Tuple, Union
)

import cloudpickle
import psutil

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.dict import Dict
from parkit.adapters.queue import Queue
from parkit.pool import (
    launch_node,
    scan_nodes,
    terminate_all_nodes,
    terminate_node
)
from parkit.storage import (
    Entity,
    objects,
    snapshot,
    transaction
)
from parkit.utility import (
    create_string_digest,
    getenv,
    polling_loop,
    resolve_path
)

logger = logging.getLogger(__name__)

class ProcessQueue(Queue):

    def encitemval(self, item):
        return cloudpickle.dumps((item.__class__, item.__getstate__()))

    def decitemval(self, encoded):
        cls, state = cloudpickle.loads(encoded)
        instance = object.__new__(cls)
        instance.__setstate__(state)
        return instance

class Process(Dict):

    def __init__(
        self,
        path: str,
        /, *,
        create: bool = True,
        bind: bool = True,
        versioned: bool = False,
        typecheck: bool = True,
        target: Optional[Callable[..., Any]] = None,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[typing.Dict[str, Any]] = None
    ):

        name, namespace = resolve_path(path)

        def on_create():
            self.__put('pid', None)
            self.__put('result', None)
            self.__put('error', None)
            self.__put('target', target)
            self.__put('args', args if args else ())
            self.__put('kwargs', kwargs if kwargs else {})
            self.__put('status', 'created')
            self.__put('node_uid', None)

        if namespace and namespace.startswith(constants.PROCESS_NAMESPACE) and '/' in namespace:
            if namespace.startswith(''.join([constants.PROCESS_NAMESPACE, '/'])):
                name = '/'.join([namespace[len(constants.PROCESS_NAMESPACE) + 1:], name])
            else:
                name = '/'.join([namespace, name])
        elif namespace and not namespace.startswith(constants.PROCESS_NAMESPACE):
            name = '/'.join([namespace, name])

        Entity.__init__(
            self, name, properties = [{}, {}], namespace = constants.PROCESS_NAMESPACE,
            create = create, bind = bind, versioned = versioned, on_create = on_create,
            typecheck = typecheck
        )

    @staticmethod
    def killall():
        cluster_uid = create_string_digest(getenv(constants.STORAGE_PATH_ENVNAME))
        terminate_all_nodes(cluster_uid)

    @staticmethod
    def get_pool_size() -> int:
        state = Dict(constants.PROCESS_STATE_PATH)
        if 'pool_size' not in state:
            state['pool_size'] = getenv(constants.PROCESS_POOL_SIZE_ENVNAME, int)
        return state['pool_size']

    @staticmethod
    def set_pool_size(size: int):
        state = Dict(constants.PROCESS_STATE_PATH)
        state['pool_size'] = size

    @staticmethod
    def clean(
        status_filter: Optional[Union[str, List[str]]] = ['finished', 'crashed', 'failed']
    ):
        paths = [path for path, _ in objects(constants.PROCESS_NAMESPACE)]
        for path in paths:
            process = Process(path, typecheck = False)
            if status_filter is None or \
            (isinstance(status_filter, list) and process.status in status_filter) or \
            process.status == status_filter:
                process.drop()

    @staticmethod
    def dir(
        status_filter: Optional[Union[str, List[str]]] = 'running'
    ) -> Iterator[Any]:
        paths = [path for path, _ in objects(constants.PROCESS_NAMESPACE)]
        for path in paths:
            process = Process(path, typecheck = False)
            if status_filter is None or \
            (isinstance(status_filter, list) and process.status in status_filter) or \
            process.status == status_filter:
                yield process

    @property
    def authkey(self):
        return None

    @property
    def sentinel(self):
        return None

    @property
    def pid(self) -> Optional[int]:
        return self.__get('pid')

    @property
    def result(self) -> Any:
        return self.__get('result')

    @property
    def error(self) -> Any:
        return self.__get('error')

    @property
    def is_alive(self) -> bool:
        return self.exists and self.status not in {'finished', 'failed', 'crashed'}

    @property
    def daemon(self) -> bool:
        return True

    @property
    def exitcode(self) -> Optional[int]:
        status = self.status
        if status in {'crashed', 'failed'}:
            return 1
        if status == 'finished':
            return 0
        return None

    @property
    def status(self) -> str:
        with snapshot(self):
            status = self.__get('status')
            if status != 'running':
                return status
            pid = self.__get('pid')
            try:
                if psutil.pid_exists(pid):
                    proc = psutil.Process(pid)
                    cmdline = proc.cmdline()
                    if self.__get('node_uid') in cmdline:
                        return 'running'
            except psutil.NoSuchProcess:
                pass
            return 'crashed'

    def __get(
        self,
        key: Any
    ) -> Any:
        key = cloudpickle.dumps(key)
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin()
            result = txn.get(key = key, default = None, db = self._Entity__userdb[1])
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)
        return cloudpickle.loads(result) if result is not None else result

    def __put(
        self,
        key: Any,
        value: Any
    ):
        key = cloudpickle.dumps(key)
        value = cloudpickle.dumps(value)
        try:
            implicit = False
            txn = thread.local.transaction
            if not txn:
                implicit = True
                txn = self._Entity__env.begin(write = True)
            assert txn.put(
                key = key, value = value, overwrite = True, append = False,
                db = self._Entity__userdb[1]
            )
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn if implicit else None)

    def run(self) -> Any:
        target = self.__get('target')
        args = self.__get('args')
        kwargs = self.__get('kwargs')
        if target:
            logger.error('process run')
            return target(*args, **kwargs)
        return None

    def start(self):
        if self.__get('status') == 'created':
            start_monitor()
            with transaction(self):
                if self.__get('status') == 'created':
                    queue = ProcessQueue(constants.PROCESS_QUEUE_PATH)
                    queue.put_nowait(self)
                    self.__put('status', 'submitted')

    def join(
        self,
        timeout: Optional[float] = None,
        polling_interval: Optional[float] = None
    ):
        default_polling_interval = getenv(constants.ADAPTER_POLLING_INTERVAL_ENVNAME, float)
        for _ in polling_loop(
            polling_interval if polling_interval is not None else \
            default_polling_interval,
            timeout = timeout
        ):
            if self.status in {'crashed', 'finished', 'failed'}:
                break

    def close(self):
        self.drop()

    def kill(self):
        self.terminate()

    def terminate(self):
        with transaction(self):
            try:
                node_uid = self.__get('node_uid')
                if node_uid and self.status == 'running':
                    cluster_uid = create_string_digest(getenv(constants.STORAGE_PATH_ENVNAME))
                    terminate_node(node_uid, cluster_uid)
            except FileNotFoundError:
                pass

    def drop(self):
        with transaction(self):
            try:
                node_uid = self.__get('node_uid')
                if node_uid and self.status == 'running':
                    cluster_uid = create_string_digest(getenv(constants.STORAGE_PATH_ENVNAME))
                    terminate_node(node_uid, cluster_uid)
            except FileNotFoundError:
                pass
            Entity.drop(self)

def start_monitor():
    state = Dict(constants.PROCESS_STATE_PATH)
    if 'monitor_last_checked' not in state:
        state['monitor_last_checked'] = datetime.datetime.now()
    else:
        now = datetime.datetime.now()
        duration = (now - state['monitor_last_checked']).seconds
        state['monitor_last_checked'] = now
        if duration < getenv(constants.MONITOR_ISALIVE_INTERVAL_ENVNAME, float):
            return
    cluster_uid = create_string_digest(getenv(constants.STORAGE_PATH_ENVNAME))
    running = scan_nodes(cluster_uid)
    if [node_uid for node_uid, _ in running if node_uid == 'monitor']:
        return
    launch_node(
        'monitor',
        'parkit.pool.monitordaemon',
        cluster_uid
    )
