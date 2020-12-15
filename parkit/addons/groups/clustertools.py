import daemoniker
import importlib
import logging
import os
import platform
import psutil
import sys
import subprocess
import tempfile
import uuid

from parkit.adapters import (
  Dict,
  WriteTransaction
)
from parkit.constants import *
from parkit.exceptions import *
from parkit.utility import *
from parkit.addons.groups.constants import *

logger = logging.getLogger(__name__)

def node_uid_from_pid_filename(filename, cluster_id):
  return filename[len(PID_FILENAME_PREFIX + cluster_id + '-'):-len(PID_FILENAME_EXTENSION)]

def is_pid_filename(filename, cluster_id):
  return filename.startswith(PID_FILENAME_PREFIX + cluster_id) and filename.endswith(PID_FILENAME_EXTENSION)

def create_pid_filepath(node_uid, cluster_id):
  return os.path.join(
    tempfile.gettempdir(), 
    PID_FILENAME_PREFIX + cluster_id + '-' + node_uid + PID_FILENAME_EXTENSION
  )
  
# def stop_cluster(settings = None):
#   try:
#     cluster_id = create_string_digest(getenv(repository_ENVNAME))
#     running_processes = scan_processes(cluster_id, settings)
#     for (node_uid, _) in running_processes:
#       if platform.system() == 'Windows':
#         terminate_node(node_uid + '-aux', cluster_id)
#       else:
#         terminate_node(node_uid, cluster_id)
#     if platform.system() == 'Windows':
#       clean_pid_directory(cluster_id)
#   except Exception as e:
#     log_and_raise(e)

def start_cluster(settings):
  processes = Dict.create_or_bind(PROCESSES_NAME, namespace = GROUPS_NAMESPACE) 
  with WriteTransaction(processes) as processes:
    if 'monitor' not in processes:
      cluster_id = create_string_digest(getenv(repository_ENVNAME))
      running_processes = scan_processes(cluster_id, settings)
      if len([node_uid for (node_uid, cmdline) in running_processes if 'monitordaemon' in cmdline[1]]) == 0:
        launch_node('monitor-{0}'.format(str(uuid.uuid4())), 'parkit.addons.groups.monitordaemon', cluster_id)

def terminate_process(pid, settings):
  pid = int(pid)
  if psutil.pid_exists(pid):
    try:
      proc = psutil.Process(pid)
      proc.terminate()
      timeout = settings['process_termination_timeout'] if settings is not None and 'process_termination_timeout' in settings else DEFAULT_PROCESS_TERMINATION_TIMEOUT
      gone, alive = psutil.wait_procs([proc], timeout = timeout)
      for proc in alive:
        proc.kill()
    except Exception as e:
      log(e)
  
def clean_pid_directory(cluster_id):
  for filename in os.listdir(tempfile.gettempdir()):
    if is_pid_filename(filename, cluster_id): 
      pid_filepath = os.path.join(tempfile.gettempdir(), filename)
      os.remove(pid_filepath)

def process_tag_exists(*tags):
  for proc in psutil.process_iter():
    try:
      cmdline = proc.cmdline()
      if any([tag in cmdline for tag in list(tags)]):
        return True
    except psutil.AccessDenied:
      pass
  return False
  
def scan_processes(cluster_id, settings):

  #
  # Capture all the pid files in temp directory. The extra Windows madness is
  # due to quirk in Deamoniker which uses two processes per (virtual) Windows daemon.
  #
  recorded_pids = dict()
  for filename in os.listdir(tempfile.gettempdir()):
    if is_pid_filename(filename, cluster_id): 
      pid_filepath = os.path.join(tempfile.gettempdir(), filename)
      with open(pid_filepath, 'rt') as file:
        pid = file.read().strip()
      node_uid = node_uid_from_pid_filename(filename, cluster_id)
      recorded_pids[pid] = (node_uid, None)
      
  #
  # Verify daemons are running and restore pid files that are missing.
  #
  for proc in psutil.process_iter():
    try:
      cmdline = proc.cmdline()
      if any([tag in cmdline for tag in [cluster_id, DAEMONIKER_WINDOWS_PROCESS_TAG]]):
        pid = str(proc.pid)
        if not pid in recorded_pids:
          if platform.system() == 'Windows':
            if cluster_id in cmdline:
              if len(proc.children()):
                node_uid = cmdline[2].split('=')[1]
              else:
                terminate_process(pid, settings)
                continue
            else:
              if psutil.pid_exists(proc.ppid()) and cluster_id in psutil.Process(proc.ppid()).cmdline():
                node_uid = psutil.Process(proc.ppid()).cmdline()[2].split('=')[1] + '-aux'
              elif not psutil.pid_exists(proc.ppid()):
                terminate_process(pid, settings)
                continue
          else:
            node_uid = cmdline[2].split('=')[1]
          pid_filepath = create_pid_filepath(node_uid, cluster_id)
          with open(pid_filepath, 'wt') as file:
            file.write(pid)
        else:
          if platform.system() == 'Windows':
            if cluster_id in cmdline:
              if len(proc.children()):
                node_uid, _ = recorded_pids[pid]
              else:
                terminate_process(pid, settings)
                continue
            else:
              if psutil.pid_exists(proc.ppid()):
                node_uid, _ = recorded_pids[pid]
              else:
                terminate_process(pid, settings)
                continue
          else:
            node_uid, _ = recorded_pids[pid]
        recorded_pids[pid] = (node_uid, cmdline)
    except psutil.AccessDenied:
      pass

  #
  # Delete pid file if daemon is not running.
  #
  for (node_uid, cmdline) in recorded_pids.values():
    if cmdline is None:
      pid_filepath = create_pid_filepath(node_uid, cluster_id)
      os.remove(pid_filepath)  

  #
  # Return list of running daemons.
  #
  running_processes = list()
  for pid, (node_uid, cmdline) in recorded_pids.items():
    if cmdline is not None:
      if platform.system() == 'Windows':
        if '-aux' not in node_uid:
          running_processes.append((node_uid, cmdline))
      else:
        running_processes.append((node_uid, cmdline))

  return running_processes

def terminate_node(node_uid, cluster_id):
  pid_filepath = create_pid_filepath(node_uid, cluster_id)
  daemoniker.send(pid_filepath, daemoniker.SIGINT)

def launch_node(node_uid, node_module, cluster_id):
  module = importlib.import_module(node_module)
  path = os.path.abspath(module.__file__) 
  subprocess.run(
    [sys.executable, path, 'uid={0}'.format(node_uid), cluster_id], 
    check = True
  )

