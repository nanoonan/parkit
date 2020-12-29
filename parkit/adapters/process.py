# pylint: disable = dangerous-default-value
import logging

from typing import (
    Any, Callable, cast, Dict, Optional, Tuple
)

import cloudpickle
import psutil

import parkit.adapters.dict as adapters_dict
import parkit.constants as constants
import parkit.storage.mixins as mixins

from parkit.adapters.queue import Queue
from parkit.pool import terminate_node
from parkit.storage import (
    Missing,
    Object,
    snapshot,
    transaction
)
from parkit.utility import (
    create_string_digest,
    getenv,
    resolve
)

logger = logging.getLogger(__name__)

class ProcessMeta(adapters_dict.DictMeta):

    def __initialize_class__(cls):
        super().__initialize_class__()
        if isinstance(cls._get, Missing):
            setattr(cls, '_get', mixins.attributes.get(
                cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., Any], cloudpickle.dumps),
                cls.decode_value if not isinstance(cls.decode_value, Missing) else \
                cast(Callable[..., Any], cloudpickle.loads)
            ))
        if isinstance(cls._put, Missing):
            setattr(cls, '_put', mixins.attributes.put(
                cls.encode_key if not isinstance(cls.encode_key, Missing) else \
                cast(Callable[..., Any], cloudpickle.dumps),
                cls.encode_value if not isinstance(cls.encode_value, Missing) else \
                cast(Callable[..., Any], cloudpickle.dumps)
            ))

    def __call__(cls, *args, **kwargs):
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)

class Process(adapters_dict.Dict, metaclass = ProcessMeta):

    def __init__(
        self, path: str, create: bool = True, bind: bool = True, versioned: bool = False,
        target: Optional[Callable[..., Any]] = None,
        args: Tuple[Any, ...] = (),
        kwargs: Dict[str, Any] = {}
    ) -> None:

        name, namespace = resolve(path, path = True)

        def on_create() -> None:
            self._put('pid', None)
            self._put('result', None)
            self._put('error', None)
            self._put('target', target)
            self._put('args', args)
            self._put('kwargs', kwargs)
            self._put('status', 'submitted')
            self._put('node_uid', None)
            queue = Queue(constants.PROCESS_QUEUE_PATH)
            queue.put(self)

        if namespace and namespace.startswith(constants.PROCESS_NAMESPACE) and '/' in namespace:
            if namespace.startswith(''.join([constants.PROCESS_NAMESPACE, '/'])):
                name = '/'.join([namespace[len(constants.PROCESS_NAMESPACE) + 1:], name])
            else:
                name = '/'.join([namespace, name])
        elif namespace and not namespace.startswith(constants.PROCESS_NAMESPACE):
            name = '/'.join([namespace, name])

        Object.__init__(
            self, name, properties = [{}], namespace = constants.PROCESS_NAMESPACE,
            create = create, bind = bind, versioned = versioned, on_create = on_create
        )

    @property
    def pid(self):
        return self._get('pid')

    @property
    def error(self):
        return self._get('error')

    @property
    def node_uid(self):
        return self._get('node_uid')

    @property
    def result(self):
        return self._get('result')

    @property
    def target(self):
        return self._get('target')

    @property
    def args(self):
        return self._get('args')

    @property
    def kwargs(self):
        return self._get('kwargs')

    @property
    def status(self) -> str:
        with snapshot(self):
            status = self._get('status')
            if status != 'running':
                return status
            pid = self._get('pid')
            if psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                cmdline = proc.cmdline()
                if self._get('node_uid') in cmdline:
                    return 'running'
            return 'crashed'

    def drop(self) -> None:
        with transaction(self):
            try:
                if self.node_uid:
                    cluster_uid = create_string_digest(getenv(constants.INSTALL_PATH_ENVNAME))
                    terminate_node(self.node_uid, cluster_uid)
            except FileNotFoundError:
                pass
            Object.drop(self)

    _get: Callable[..., Any] = Missing()

    _put: Callable[..., None] = Missing()
