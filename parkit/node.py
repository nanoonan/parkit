# pylint: disable = broad-except
import importlib
import logging
import os
import platform
import sys
import subprocess
import threading

from typing import (
    Dict, Optional
)

import psutil

import parkit.constants as constants

from parkit.utility import getenv

logger = logging.getLogger(__name__)

def terminate_all_nodes(cluster_uid: str):
    def run():
        for proc in psutil.process_iter(['environ', 'pid']):
            try:
                env = proc.info['environ']
                if env and constants.NODE_UID_ENVNAME in env and \
                constants.CLUSTER_UID_ENVNAME in env:
                    if env[constants.CLUSTER_UID_ENVNAME] == cluster_uid:
                        pid = proc.info['pid']
                        terminate_process(
                            pid,
                            getenv(constants.PROCESS_TERMINATION_TIMEOUT_ENVNAME, float)
                        )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            except Exception:
                logger.exception('error terminating node')
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
        except Exception:
            logger.exception('error terminating process')

def terminate_node(
    node_uid: str,
    cluster_uid: str
):
    def run():
        for proc in psutil.process_iter(['environ', 'pid']):
            try:
                env = proc.info['environ']
                if env and constants.NODE_UID_ENVNAME in env and \
                constants.CLUSTER_UID_ENVNAME in env:
                    if env[constants.CLUSTER_UID_ENVNAME] == cluster_uid and \
                    env[constants.NODE_UID_ENVNAME] == node_uid:
                        pid = proc.info['pid']
                        terminate_process(
                            pid,
                            getenv(
                                constants.PROCESS_TERMINATION_TIMEOUT_ENVNAME,
                                float
                            )
                        )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            except Exception:
                logger.exception('error termining node')
    threading.Thread(target = run).start()

def launch_node(
    node_uid: str,
    node_module: str,
    cluster_uid: str,
    environment: Optional[Dict[str, str]] = None
):
    def run():
        try:
            module = importlib.import_module(node_module)
            path = os.path.abspath(module.__file__)
            env = os.environ.copy()
            env[constants.NODE_UID_ENVNAME] = node_uid
            env[constants.CLUSTER_UID_ENVNAME] = cluster_uid
            if environment:
                for name, value in environment.items():
                    if name not in [
                        constants.NODE_UID_ENVNAME, constants.CLUSTER_UID_ENVNAME,
                    ]:
                        env[name] = value
            if platform.system() == 'Windows':
                create_new_process_group = 0x00000200
                detached_process = 0x00000008
                subprocess.Popen(
                    [sys.executable, path], stdin = subprocess.PIPE,
                    stdout = subprocess.PIPE, stderr = subprocess.PIPE,  env = env,
                    creationflags = detached_process | create_new_process_group
                )
            else:
                raise NotImplementedError()
        except Exception:
            logger.error('error launching node')
    threading.Thread(target = run).start()
