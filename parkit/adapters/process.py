# pylint: disable = dangerous-default-value
import logging

from typing import (
    Any, ByteString, Callable, cast, Dict, Optional, Tuple
)

import cloudpickle
import psutil

import parkit.constants as constants
import parkit.storage.mixins as mixins

from parkit.adapters.attribute import (
    Attr,
    Attributes
)
from parkit.adapters.queue import Queue
from parkit.pool import terminate_node
from parkit.storage import (
    Missing,
    Object,
    ObjectMeta,
    snapshot,
    transaction
)
from parkit.utility import (
    create_string_digest,
    getenv,
    resolve
)

logger = logging.getLogger(__name__)

class ProcessMeta(ObjectMeta):

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

class Process(Attributes, Object, metaclass = ProcessMeta):

    pid = Attr(readonly = True)
    error = Attr(readonly = True)
    node_uid = Attr(readonly = True)
    result = Attr(readonly = True)
    target = Attr(readonly = True)
    args = Attr(readonly = True)
    kwargs = Attr(readonly = True)

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

        Attributes.__init__(self)
        Object.__init__(
            self, name, properties = [], namespace = constants.PROCESS_NAMESPACE,
            create = create, bind = bind, on_create = on_create
        )

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
            super().drop()

    encode_key: Callable[..., ByteString] = Missing()

    decode_value: Callable[..., Any] = Missing()

    encode_value: Callable[..., ByteString] = Missing()
