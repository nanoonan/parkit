import logging
import os
import uuid

#from parkit.addons.groups import Group
from parkit.constants import *
from parkit.exceptions import *
from parkit.utility import *

logger = logging.getLogger(__name__)

def init(repository = None, groups = False, profile = 'default'):
  try:
    if repository is None:
      repository = os.getenv(REPOSITORY_ENVNAME)
      if repository is None:
        raise InvalidEnvironment()
    repository = os.path.abspath(repository)
    if os.path.exists(repository):
      if not os.path.isdir(repository):
        raise InvalidPath()
    try:
      os.makedirs(repository)
    except FileExistsError:
      pass
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
      setenv(INSTALLATION_UUID_ENVNAME, create_string_digest(repository))
    if not envexists(REPOSITORY_ENVNAME):
      setenv(REPOSITORY_ENVNAME, repository)
    if not envexists(PROCESS_INSTANCE_UUID_ENVNAME):
      setenv(PROCESS_INSTANCE_UUID_ENVNAME, str(uuid.uuid4()))
    #if groups:
    #  Group.init()
  except Exception as e:
    log_and_raise(e)

