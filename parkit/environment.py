import logging
import os
import uuid

from parkit.constants import *
from parkit.exceptions import *
from parkit.utility import *

logger = logging.getLogger(__name__)

def init(install_path = None, address = None, port = None, profile = 'default'):
  try:
    if install_path is None:
      install_path = os.getenv(INSTALL_PATH_ENVNAME)
      if install_path is None:
        raise InvalidEnvironment()
    install_path = os.path.abspath(install_path)
    if os.path.exists(install_path):
      if not os.path.isdir(install_path):
        raise InvalidPath()
    try:
      os.makedirs(install_path)
    except FileExistsError:
      pass
    if address is not None:
      setenv(ADDRESS_ENVNAME, address)
    if port is not None:
      setenv(PORT_ENVNAME, port)
    if profile is None:
      if envexists(LMDB_PROFILE_ENVNAME):
        profile = getenv(LMDB_PROFILE_ENVNAME)
      else:
        profile = 'default'
    for name, default in lmdb_profiles[profile]:
      if not envexists(name):
        setenv(name, default)
      else:
        checkenv(name, type(default))
    if not envexists(INSTALLATION_UUID_ENVNAME):
      setenv(INSTALLATION_UUID_ENVNAME, create_string_digest(install_path))
    if not envexists(INSTALL_PATH_ENVNAME):
      setenv(INSTALL_PATH_ENVNAME, install_path)
    if not envexists(PROCESS_INSTANCE_UUID_ENVNAME):
      setenv(PROCESS_INSTANCE_UUID_ENVNAME, str(uuid.uuid4()))
  except Exception as e:
    log_and_raise(e)

