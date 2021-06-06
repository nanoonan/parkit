import importlib
import logging
import os
import sys
import subprocess
import tempfile
import threading

from typing import (
    Dict, List, Optional, Tuple
)

import daemoniker
import psutil

import parkit.constants as constants

from parkit.utility import (
    create_string_digest,
    getenv
)

logger = logging.getLogger(__name__)

def node_uid_from_pid_filename(
    filename: str,
    cluster_uid: str
) -> str:
    return filename[
        len(constants.PID_FILENAME_PREFIX + cluster_uid + '-'): \
        -len(constants.PID_FILENAME_EXTENSION)
    ]

def is_pid_filename(
    filename: str,
    cluster_uid: str
) -> bool:
    return filename.startswith(constants.PID_FILENAME_PREFIX + cluster_uid) and \
    filename.endswith(constants.PID_FILENAME_EXTENSION)

def create_pid_filepath(
    node_uid: str,
    cluster_uid: str
) -> str:
    return os.path.join(
        tempfile.gettempdir(),
        constants.PID_FILENAME_PREFIX + cluster_uid + '-' + \
        node_uid + constants.PID_FILENAME_EXTENSION
    )

def terminate_all_nodes(storage_path: str):

    def run():
        cluster_uid = create_string_digest(storage_path)
        for proc in psutil.process_iter(['environ', 'pid']):
            try:
                env = proc.info['environ']
                if env and constants.NODE_UID_ENVNAME in env and constants.CLUSTER_UID_ENVNAME in env:
                    if env[constants.CLUSTER_UID_ENVNAME] == cluster_uid:
                        if '__INVOKE_DAEMON__' in env or '__PARKIT_DAEMON__' in env:
                            pid = proc.info['pid']
                            terminate_process(
                                pid,
                                getenv(constants.PROCESS_TERMINATION_TIMEOUT_ENVNAME, float)
                            )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    threading.Thread(target = run).start()

def terminate_process(
    pid: int,
    termination_timeout: float
):
    if psutil.pid_exists(pid):
        try:
            proc = psutil.Process(pid)
            proc.terminate()
            _, alive = psutil.wait_procs([proc], timeout = termination_timeout)
            for proc in alive:
                proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

def get_recorded_pids(cluster_uid: str) -> Dict[int, Tuple[str, bool]]:
    recorded_pids: Dict[int, Tuple[str, bool]] = {}
    for filename in os.listdir(tempfile.gettempdir()):
        if is_pid_filename(filename, cluster_uid):
            pid_filepath = os.path.join(tempfile.gettempdir(), filename)
            try:
                with open(pid_filepath, 'rt') as file:
                    pid = int(file.read().strip())
                node_uid = node_uid_from_pid_filename(filename, cluster_uid)
                recorded_pids[pid] = (node_uid, False)
            except ValueError:
                pass
            except OSError:
                logger.exception('cluster error')
    return recorded_pids

def scan_nodes(storage_path: str) -> List[str]:

    cluster_uid = create_string_digest(storage_path)

    #
    # Try to capture all the active pid files in temp directory.
    #
    recorded_pids = get_recorded_pids(cluster_uid)

    #
    # Verify daemons are running and try to restore pid files that are missing.
    #
    for proc in psutil.process_iter(['environ', 'pid']):
        try:
            env = proc.info['environ']
            if env and constants.NODE_UID_ENVNAME in env and constants.CLUSTER_UID_ENVNAME in env:
                if env[constants.CLUSTER_UID_ENVNAME] == cluster_uid:
                    if '__INVOKE_DAEMON__' in env or '__PARKIT_DAEMON__' in env:
                        pid = proc.info['pid']
                        if pid not in recorded_pids:
                            node_uid = env[constants.NODE_UID_ENVNAME]
                            pid_filepath = create_pid_filepath(node_uid, cluster_uid)
                            try:
                                logger.info('restore pidfile %s %i', pid_filepath, pid)
                                with open(pid_filepath, 'wt') as file:
                                    file.write(str(pid))
                            except OSError:
                                logger.exception('cluster error')
                        else:
                            node_uid, _ = recorded_pids[pid]
                        recorded_pids[pid] = (node_uid, True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    #
    # Try to delete pid file if daemon is not running.
    #
    for (node_uid, validated) in recorded_pids.values():
        if not validated:
            pid_filepath = create_pid_filepath(node_uid, cluster_uid)
            try:
                os.remove(pid_filepath)
            except FileNotFoundError:
                pass
            except OSError:
                logger.exception('cluster error')

    #
    # Return list of running daemons.
    #
    running = []
    for pid, (node_uid, validated) in recorded_pids.items():
        if validated:
            running.append(node_uid)

    return running

def terminate_node(
    node_uid: str,
    storage_path: str
):

    def run():
        cluster_uid = create_string_digest(storage_path)
        try:
            pid_filepath = create_pid_filepath(node_uid, cluster_uid)
            daemoniker.send(pid_filepath, daemoniker.SIGINT)
            return True
        except Exception:
            for proc in psutil.process_iter(['environ', 'pid']):
                try:
                    env = proc.info['environ']
                    if env and constants.NODE_UID_ENVNAME in env and \
                    constants.CLUSTER_UID_ENVNAME in env:
                        if env[constants.CLUSTER_UID_ENVNAME] == cluster_uid:
                            if '__INVOKE_DAEMON__' in env or '__PARKIT_DAEMON__' in env:
                                pid = proc.info['pid']
                                terminate_process(
                                    pid,
                                    getenv(
                                        constants.PROCESS_TERMINATION_TIMEOUT_ENVNAME,
                                        float
                                    )
                                )
                                return True
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            return False
    threading.Thread(target = run).start()

def launch_node(
    node_uid: str,
    node_module: str,
    storage_path: str,
    environment: Optional[Dict[str, str]] = None
):
    def run():
        cluster_uid = create_string_digest(storage_path)
        module = importlib.import_module(node_module)
        path = os.path.abspath(module.__file__)
        env = os.environ.copy()
        env[constants.STORAGE_PATH_ENVNAME] = storage_path
        env[constants.NODE_UID_ENVNAME] = node_uid
        env[constants.CLUSTER_UID_ENVNAME] = cluster_uid
        if environment:
            for name, value in environment.items():
                if name not in [
                    constants.NODE_UID_ENVNAME, constants.CLUSTER_UID_ENVNAME,
                    constants.STORAGE_PATH_ENVNAME
                ]:
                    env[name] = value
        subprocess.run(
            [sys.executable, path],
            check = True, env = env
        )
    threading.Thread(target = run).start()
