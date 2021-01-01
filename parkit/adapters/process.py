# pylint: disable = dangerous-default-value, no-value-for-parameter
import logging
import typing

from typing import (
    Any, ByteString, Callable, cast, Optional, Tuple
)

import cloudpickle
import psutil

import parkit.constants as constants

from parkit.adapters.dict import Dict
from parkit.adapters.queue import Queue
from parkit.pool import terminate_node
from parkit.storage import (
    Entity,
    snapshot,
    transaction
)
from parkit.utility import (
    create_string_digest,
    getenv,
    resolve_path
)

logger = logging.getLogger(__name__)

class Process(Dict):

    encode_attrval: Callable[..., ByteString] = \
    cast(Callable[..., ByteString], staticmethod(cloudpickle.dumps))

    decode_attrval: Callable[..., Any] = \
    cast(Callable[..., Any], staticmethod(cloudpickle.loads))

    def __init__(
        self,
        path: str,
        create: bool = True,
        bind: bool = True,
        versioned: bool = False,
        target: Optional[Callable[..., Any]] = None,
        args: Tuple[Any, ...] = (),
        kwargs: typing.Dict[str, Any] = {}
    ) -> None:

        name, namespace = resolve_path(path)

        def on_create() -> None:
            self._putattr('pid', None)
            self._putattr('result', None)
            self._putattr('error', None)
            self._putattr('target', target)
            self._putattr('args', args)
            self._putattr('kwargs', kwargs)
            self._putattr('status', 'submitted')
            self._putattr('node_uid', None)
            queue = Queue(constants.PROCESS_QUEUE_PATH)
            queue.put(self)

        if namespace and namespace.startswith(constants.PROCESS_NAMESPACE) and '/' in namespace:
            if namespace.startswith(''.join([constants.PROCESS_NAMESPACE, '/'])):
                name = '/'.join([namespace[len(constants.PROCESS_NAMESPACE) + 1:], name])
            else:
                name = '/'.join([namespace, name])
        elif namespace and not namespace.startswith(constants.PROCESS_NAMESPACE):
            name = '/'.join([namespace, name])

        Entity.__init__(
            self, name, properties = [{}], namespace = constants.PROCESS_NAMESPACE,
            create = create, bind = bind, versioned = versioned, on_create = on_create
        )

    @property
    def pid(self):
        return self._getattr('pid')

    @property
    def error(self):
        return self._getattr('error')

    @property
    def node_uid(self):
        return self._getattr('node_uid')

    @property
    def result(self):
        return self._getattr('result')

    @property
    def target(self):
        return self._getattr('target')

    @property
    def args(self):
        return self._getattr('args')

    @property
    def kwargs(self):
        return self._getattr('kwargs')

    @property
    def status(self) -> str:
        with snapshot(self):
            status = self._getattr('status')
            if status != 'running':
                return status
            pid = self._getattr('pid')
            if psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                cmdline = proc.cmdline()
                if self._getattr('node_uid') in cmdline:
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
            Entity.drop(self)
