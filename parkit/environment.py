import logging
import os
import parkit.constants as constants
import parkit.profiles as profiles
import tempfile
import threading

from parkit.exceptions import (
  log,
  log_and_raise
)
from parkit.process.processtools import (
  launch_node,
  scan_processes,
  terminate_all_nodes
)
from parkit.storage import lock
from parkit.utility import (
  checkenv,
  create_string_digest,
  envexists,
  getenv,
  setenv
)

logger = logging.getLogger(__name__)
  
env_lock = threading.Lock()

initialized = False

def running():
  cluster_uid = create_string_digest(getenv(constants.INSTALL_PATH_ENVNAME))
  with lock('monitor'):
    print(scan_processes(cluster_uid))

def start_cluster():
  try:
    cluster_uid = create_string_digest(getenv(constants.INSTALL_PATH_ENVNAME))
    with lock('monitor'):
      running = scan_processes(cluster_uid)
      for node_uid, _ in running:
        if node_uid == 'monitor':
          return
      launch_node('monitor', 'parkit.process.monitordaemon', cluster_uid) 
  except Exception as e:
    log(e)
  
def stop_cluster():
  try:
    cluster_uid = create_string_digest(getenv(constants.INSTALL_PATH_ENVNAME))
    with lock('monitor'):
      terminate_all_nodes(cluster_uid)
  except Exception as e:
    log(e)

def set_environment(install_path = None):
  global initialized
  with env_lock:
    if not initialized:
      try:
        if install_path is None:
          install_path = os.getenv(constants.INSTALL_PATH_ENVNAME)
        install_path = os.path.abspath(install_path)
        if os.path.exists(install_path):
          if not os.path.isdir(install_path):
            raise ValueError('Install_path is not a directory')
        else:
          os.makedirs(install_path)
        for name, default in profiles.lmdb_profiles['persistent'].copy().items():
          if envexists(name):
            if checkenv(name, type(default)):
              profiles.lmdb_profiles['persistent'][name] = getenv(name, type(default)) 
        setenv(constants.INSTALL_PATH_ENVNAME, install_path)
        initialized = True
      except Exception as e:
        log_and_raise(e)

if envexists(constants.INSTALL_PATH_ENVNAME):
  set_environment(install_path = getenv(constants.INSTALL_PATH_ENVNAME))
else:
  try:
    os.makedirs(os.path.join(tempfile.gettempdir(), constants.PARKIT_TEMP_INSTALLATION_DIRNAME))
  except FileExistsError:
    pass
  set_environment(
    install_path = os.path.join(tempfile.gettempdir(), constants.PARKIT_TEMP_INSTALLATION_DIRNAME)
  )