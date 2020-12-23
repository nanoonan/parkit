import threading
import logging

logger = logging.getLogger(__name__)

class CursorDict(dict):

  def __getitem__(self, key):
    if not dict.__contains__(self, key):
      cursor = local.transaction.cursor(db = local.databases[key])
      dict.__setitem__(self, key, cursor)
      return cursor
    return dict.__getitem__(self, key)

class ThreadLocalVars(threading.local):
  
  def __init__(self):
    super().__init__()
    self.__dict__['transaction'] = None
    self.__dict__['cursors'] = None
    self.__dict__['changed'] = None
    self.__dict__['transaction_stack'] = []
    self.__dict__['changed_stack'] = []
    self.__dict__['cursors_stack'] = []
    self.__dict__['property_stack'] = []
    self.__dict__['databases'] = {}

local = ThreadLocalVars()

