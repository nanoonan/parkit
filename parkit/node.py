# pylint: disable = broad-except
#
# reviewed: 6/14/21
#
import importlib
import logging
import os
import platform
import sys
import subprocess

from typing import (
    Dict, List, Optional
)

import psutil

import parkit.constants as constants

from parkit.utility import getenv

logger = logging.getLogger(__name__)

def terminate_all_nodes(
    cluster_uid: str,
    /, *,
    priority_filter: Optional[List[str]] = None
):
    if not cluster_uid:
        raise ValueError()

    def get_nodes():
        nodes = []
        for proc in psutil.process_iter(['environ', 'pid']):
            try:
                env = proc.info['environ']
                if env and constants.CLUSTER_UID_ENVNAME in env and \
                constants.NODE_UID_ENVNAME in env:
                    if env[constants.CLUSTER_UID_ENVNAME] == cluster_uid:
                        nodes.append((
                            env[constants.NODE_UID_ENVNAME],
                            proc.info['pid']
                        ))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return nodes

    try:
        if priority_filter is not None:
            nodes = get_nodes()
            priority_nodes = [
                (node_uid, pid) for node_uid, pid in nodes \
                if [True for match in priority_filter if match in node_uid]
            ]
            for node_uid, pid in priority_nodes:
                terminate_process(
                    node_uid,
                    pid,
                    getenv(
                        constants.PROCESS_TERMINATION_TIMEOUT_ENVNAME,
                        float
                    )
                )
        for node_uid, pid in get_nodes():
            terminate_node(node_uid, pid)
    except Exception:
        logger.exception('error terminating cluster')

def terminate_process(
    node_uid: str,
    pid: int,
    termination_timeout: float
):
    if psutil.pid_exists(pid):
        try:
            proc = psutil.Process(pid)
            env = proc.environ()
            if env and constants.NODE_UID_ENVNAME in env:
                if env[constants.NODE_UID_ENVNAME] == node_uid:
                    proc.terminate()
                    _, alive = psutil.wait_procs([proc], timeout = termination_timeout)
                    for proc in alive:
                        proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

def is_running(
    node_uid: str,
    pid: Optional[int] = None
) -> bool:
    if not node_uid:
        raise ValueError()
    if pid is not None:
        try:
            if psutil.pid_exists(pid):
                proc = psutil.Process(pid)
                env = proc.environ()
                if env and constants.NODE_UID_ENVNAME in env:
                    if env[constants.NODE_UID_ENVNAME] == node_uid:
                        return proc.is_running()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        return False
    for proc in psutil.process_iter(['environ', 'pid']):
        try:
            env = proc.info['environ']
            if env and constants.NODE_UID_ENVNAME in env:
                if env[constants.NODE_UID_ENVNAME] == node_uid:
                    return proc.is_running()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return False

def terminate_node(
    node_uid: str,
    pid: Optional[int] = None
):
    if not node_uid:
        raise ValueError()
    try:
        if pid is not None:
            terminate_process(
                node_uid,
                pid,
                getenv(
                    constants.PROCESS_TERMINATION_TIMEOUT_ENVNAME,
                    float
                )
            )
        else:
            for proc in psutil.process_iter(['environ', 'pid']):
                try:
                    env = proc.info['environ']
                    if env and constants.NODE_UID_ENVNAME in env:
                        if env[constants.NODE_UID_ENVNAME] == node_uid:
                            terminate_process(
                                node_uid,
                                proc.info['pid'],
                                getenv(
                                    constants.PROCESS_TERMINATION_TIMEOUT_ENVNAME,
                                    float
                                )
                            )
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
    except Exception:
        logger.exception('error terminating node')

def launch_node(
    node_uid: str,
    node_module: str,
    cluster_uid: str,
    environment: Optional[Dict[str, str]] = None
) -> int:
    if not node_uid or not node_module or not cluster_uid:
        raise ValueError()
    module = importlib.import_module(node_module)
    path = os.path.abspath(module.__file__)

    if platform.system() == 'Windows':
        try:
            env = os.environ.copy()
            env[constants.NODE_UID_ENVNAME] = node_uid
            env[constants.CLUSTER_UID_ENVNAME] = cluster_uid
            if environment:
                for name, value in environment.items():
                    if name not in [
                        constants.NODE_UID_ENVNAME, constants.CLUSTER_UID_ENVNAME,
                    ]:
                        env[name] = value
            create_new_process_group = 0x00000200
            detached_process = 0x00000008
            return subprocess.Popen(
                [sys.executable, path], env = env,
                stdin = subprocess.DEVNULL, stderr = subprocess.STDOUT,
                stdout = subprocess.DEVNULL,
                creationflags = detached_process | create_new_process_group
            ).pid
        except Exception as exc:
            logger.error('error launching node')
            raise exc
    else:
        raise NotImplementedError()
