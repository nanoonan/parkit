import logging
import os
import typing

from typing import (
    Any, Optional, Union
)

import psutil

import parkit.constants as constants
import parkit.storage.threadlocal as thread

from parkit.adapters.dict import Dict
from parkit.storage.context import transaction_context
from parkit.storage.site import (
    get_site_uuid,
    import_site
)
from parkit.utility import (
    envexists,
    getenv
)

logger = logging.getLogger(__name__)

import_site(getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str), create = True)

class PidTable(Dict):

    def __init__(
        self,
        path: Optional[str] = constants.PIDTABLE_DICT_PATH,
        /, *,
        site_uuid: Optional[str] = \
        get_site_uuid(getenv(constants.GLOBAL_SITE_STORAGE_PATH_ENVNAME, str)),
        create: bool = True,
        bind: bool = True
    ):

        self.__counter: int

        def on_init(created: bool):
            if created:
                self.__counter = 0

        super().__init__(
            path, on_init = on_init,
            site_uuid = site_uuid,
            create = create, bind = bind
        )

    def __enter__(self):
        thread.local.context.push(self._env, True, False)
        return self

    def __exit__(self, error_type: type, error: Optional[Any], traceback: Any):
        thread.local.context.pop(self._env, abort = error is not None)

    def next_counter_value(self):
        with transaction_context(self._env, write = True):
            value = self.__counter
            self.__counter += 1
            return value

    def set_pid_entry(
        self, *,
        pid: int = os.getpid(),
        process_uid: str = getenv(constants.PROCESS_UID_ENVNAME, str),
        node_uid = getenv(constants.NODE_UID_ENVNAME, str) \
        if envexists(constants.NODE_UID_ENVNAME) else None,
        cluster_uid = getenv(constants.CLUSTER_UID_ENVNAME, str) \
        if envexists(constants.CLUSTER_UID_ENVNAME) else None
    ):
        self[pid] = dict(
            create_time = psutil.Process(pid).create_time(),
            process_uid = process_uid,
            node_uid = node_uid,
            cluster_uid = cluster_uid
        )

    def get_snapshot(self) -> \
    typing.Dict[int, typing.Dict[str, Union[float, Optional[str]]]]:
        with transaction_context(self._env, write = True):
            active_pids = []
            recorded_pids = list(self.keys())
            for proc in psutil.process_iter(['create_time', 'pid']):
                try:
                    pid = proc.info['pid']
                    active_pids.append(pid)
                    if pid in recorded_pids:
                        create_time = self[pid]['create_time']
                        if create_time < proc.info['create_time']:
                            del self[pid]
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            for pid in set(recorded_pids).difference(set(active_pids)):
                del self[pid]
            return dict(self)

pidtable: PidTable = PidTable()
