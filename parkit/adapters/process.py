# pylint: disable = dangerous-default-value
import logging

from typing import (
    Any, Callable, Dict, Optional, Tuple
)

import cloudpickle
import psutil

import parkit.constants as constants

from parkit.adapters.attributes import (
    Attr,
    Attributes
)
from parkit.adapters.metadata import Metadata
from parkit.adapters.queue import Queue
from parkit.pool import terminate_node
from parkit.storage import (
    LMDBObject,
    snapshot,
    transaction
)
from parkit.utility import (
    create_string_digest,
    getenv,
    resolve
)

logger = logging.getLogger(__name__)

ATTRS_INDEX = 0

class Process(LMDBObject, Attributes, Metadata):

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
            self._put_attribute('_pid', None)
            self._put_attribute('_result', None)
            self._put_attribute('_error', None)
            self._put_attribute('_target', target)
            self._put_attribute('_args', args)
            self._put_attribute('_kwargs', kwargs)
            self._put_attribute('_status', 'submitted')
            self._put_attribute('_node_uid', None)
            queue = Queue(constants.PROCESS_QUEUE_PATH)
            queue.put(self)

        if namespace and namespace.startswith(constants.PROCESS_NAMESPACE) and '/' in namespace:
            if namespace.startswith(''.join([constants.PROCESS_NAMESPACE, '/'])):
                name = '/'.join([namespace[len(constants.PROCESS_NAMESPACE) + 1:], name])
            else:
                name = '/'.join([namespace, name])
        elif namespace and not namespace.startswith(constants.PROCESS_NAMESPACE):
            name = '/'.join([namespace, name])

        LMDBObject.__init__(
            self, name, properties = [{}], namespace = constants.PROCESS_NAMESPACE,
            create = create, bind = bind, versioned = versioned, on_create = on_create
        )
        Attributes.__init__(self)
        Metadata.__init__(self)

    @property
    def status(self) -> str:
        with snapshot(self):
            status = self._get_attribute('_status')
            if status != 'running':
                return status
            pid = self._get_attribute('_pid')
            if psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                cmdline = proc.cmdline()
                if self._get_attribute('_node_uid') in cmdline:
                    return 'running'
            return 'crashed'

    def _bind(self, *_: Any) -> None:
        Attributes._bind(
            self, ATTRS_INDEX,
            encode_value = cloudpickle.dumps,
            decode_value = cloudpickle.loads
        )
        Metadata._bind(self)

    def drop(self) -> None:
        with transaction(self):
            try:
                if self.node_uid:
                    cluster_uid = create_string_digest(getenv(constants.INSTALL_PATH_ENVNAME))
                    terminate_node(self.node_uid, cluster_uid)
            except FileNotFoundError:
                pass
            super().drop()
