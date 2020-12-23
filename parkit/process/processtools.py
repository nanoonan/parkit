import daemoniker
import importlib
import logging
import os
import psutil
import sys
import subprocess
import tempfile
import uuid

from parkit.constants import *
from parkit.exceptions import *
from parkit.utility import *

logger = logging.getLogger(__name__)

def node_uid_from_pid_filename(filename, cluster_uid):
  return filename[len(PID_FILENAME_PREFIX + cluster_uid + '-'):-len(PID_FILENAME_EXTENSION)]

def is_pid_filename(filename, cluster_uid):
  return filename.startswith(PID_FILENAME_PREFIX + cluster_uid) and filename.endswith(PID_FILENAME_EXTENSION)

def create_pid_filepath(node_uid, cluster_uid):
  return os.path.join(
    tempfile.gettempdir(), 
    PID_FILENAME_PREFIX + cluster_uid + '-' + node_uid + PID_FILENAME_EXTENSION
  )

def terminate_all_nodes(cluster_uid):
  running = scan_processes(cluster_uid)
  for node_uid, _ in running:
    terminate_node(node_uid, cluster_uid)
  
def terminate_process(pid, settings):
  if psutil.pid_exists(pid):
    try:
      proc = psutil.Process(pid)
      proc.terminate()
      timeout = settings['process_termination_timeout'] if settings is not None and 'process_termination_timeout' in settings else DEFAULT_PROCESS_TERMINATION_TIMEOUT
      gone, alive = psutil.wait_procs([proc], timeout = timeout)
      for proc in alive:
        proc.kill()
    except (psutil.NoSuchProcess, psutil.AccessDenied):
      pass
    except Exception as e:
      log(e)
  
def clean_pid_directory(cluster_uid):
  for filename in os.listdir(tempfile.gettempdir()):
    if is_pid_filename(filename, cluster_uid): 
      pid_filepath = os.path.join(tempfile.gettempdir(), filename)
      os.remove(pid_filepath)
  
def scan_processes(cluster_uid, settings = None):

  #
  # Capture all the pid files in temp directory. 
  #
  recorded_pids = {}
  for filename in os.listdir(tempfile.gettempdir()):
    if is_pid_filename(filename, cluster_uid): 
      pid_filepath = os.path.join(tempfile.gettempdir(), filename)
      with open(pid_filepath, 'rt') as file:
        pid = file.read().strip()
      node_uid = node_uid_from_pid_filename(filename, cluster_uid)
      recorded_pids[pid] = (node_uid, None)
      
  #
  # Verify daemons are running and restore pid files that are missing. 
  #
  for proc in psutil.process_iter(['cmdline', 'pid', 'ppid']):
    try:
      cmdline = proc.info['cmdline']
      if cmdline and len(cmdline) and any([tag in cmdline for tag in [cluster_uid, DAEMONIKER_WINDOWS_PROCESS_TAG]]):
        pid = str(proc.info['pid'])
        if not pid in recorded_pids:
          node_uid = cmdline[2]
          pid_filepath = create_pid_filepath(node_uid, cluster_uid)
          with open(pid_filepath, 'wt') as file:
            file.write(pid)
        else:
          node_uid, _ = recorded_pids[pid]
        recorded_pids[pid] = (node_uid, cmdline)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
      pass

  #
  # Delete pid file if daemon is not running.
  #
  for (node_uid, cmdline) in recorded_pids.values():
    if cmdline is None:
      pid_filepath = create_pid_filepath(node_uid, cluster_uid)
      os.remove(pid_filepath)  

  #
  # Return list of running daemons.
  #
  running_processes = []
  for pid, (node_uid, cmdline) in recorded_pids.items():
    if cmdline is not None:
      running_processes.append((node_uid, cmdline))

  return running_processes

def terminate_node(node_uid, cluster_uid):
  pid_filepath = create_pid_filepath(node_uid, cluster_uid)
  daemoniker.send(pid_filepath, daemoniker.SIGINT)

def launch_node(node_uid, node_module, cluster_uid):
  print('launch', node_uid, node_module, cluster_uid)
  module = importlib.import_module(node_module)
  path = os.path.abspath(module.__file__) 
  subprocess.run(
    [sys.executable, path, node_uid, cluster_uid], 
    check = True
  )

