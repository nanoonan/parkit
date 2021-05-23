import importlib
import logging
import os
import sys
import subprocess
import tempfile

from typing import (
    Any, Dict, List, Optional, Tuple
)

import daemoniker
import psutil

import parkit.constants as constants

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

def terminate_all_nodes(cluster_uid: str):
    running = scan_nodes(cluster_uid)
    for node_uid, _ in running:
        terminate_node(node_uid, cluster_uid)

def terminate_process(
    pid: int,
    process_termination_timeout: float = 1
):
    if psutil.pid_exists(pid):
        try:
            proc = psutil.Process(pid)
            proc.terminate()
            _, alive = psutil.wait_procs([proc], timeout = process_termination_timeout)
            for proc in alive:
                proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

def get_recorded_pids(cluster_uid: str) -> Dict[str, Tuple[str, List[str]]]:
    recorded_pids: Dict[str, Tuple[str, List[str]]] = {}
    for filename in os.listdir(tempfile.gettempdir()):
        if is_pid_filename(filename, cluster_uid):
            pid_filepath = os.path.join(tempfile.gettempdir(), filename)
            try:
                with open(pid_filepath, 'rt') as file:
                    pid = file.read().strip()
                node_uid = node_uid_from_pid_filename(filename, cluster_uid)
                recorded_pids[pid] = (node_uid, [])
            except OSError:
                pass
    return recorded_pids

def scan_nodes(cluster_uid: str) -> List[Tuple[str, List[str]]]:

    #
    # Try to capture all the pid files in temp directory.
    #
    recorded_pids = get_recorded_pids(cluster_uid)

    #
    # Verify daemons are running and try to restore pid files that are missing.
    #
    for proc in psutil.process_iter(['cmdline', 'pid', 'ppid']):
        try:
            cmdline = proc.info['cmdline']
            if cmdline and any(tag in cmdline for tag in [cluster_uid]):
                pid = str(proc.info['pid'])
                if pid not in recorded_pids:
                    node_uid = cmdline[2]
                    pid_filepath = create_pid_filepath(node_uid, cluster_uid)
                    try:
                        with open(pid_filepath, 'wt') as file:
                            file.write(pid)
                    except OSError:
                        pass
                else:
                    node_uid, _ = recorded_pids[pid]
                recorded_pids[pid] = (node_uid, cmdline)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    #
    # Try to delete pid file if daemon is not running.
    #
    for (node_uid, cmdline) in recorded_pids.values():
        if not cmdline:
            pid_filepath = create_pid_filepath(node_uid, cluster_uid)
            try:
                os.remove(pid_filepath)
            except OSError:
                pass

    #
    # Return list of running daemons as (node_uid, cmdline).
    #
    running_processes = []
    for pid, (node_uid, cmdline) in recorded_pids.items():
        if cmdline:
            running_processes.append((node_uid, cmdline))

    return running_processes

def terminate_node(
    node_uid: str,
    cluster_uid: str
) -> Optional[int]:
    try:
        pid_filepath = create_pid_filepath(node_uid, cluster_uid)
        with open(pid_filepath, 'rt') as file:
            pid = file.read().strip()
        daemoniker.send(pid_filepath, daemoniker.SIGINT)
        return int(pid)
    except OSError:
        return None

def launch_node(
    node_uid: str,
    node_module: str,
    cluster_uid: str,
    *args: Any
):
    module = importlib.import_module(node_module)
    path = os.path.abspath(module.__file__)
    subprocess.run(
        [sys.executable, path, node_uid, cluster_uid, *[str(arg) for arg in args]],
        check = True
    )
