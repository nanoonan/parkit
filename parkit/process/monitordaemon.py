import daemoniker
import logging
import os
import parkit.constants as constants
import platform
import psutil
import sys
import time
import uuid

from parkit.exceptions import log
from parkit.storage import objects
from parkit.process.processtools import (
  create_pid_filepath,
  launch_node,
  scan_processes,
  terminate_node
)
from parkit.utility import create_string_digest
from parkit.storage import (
  lock,
  snapshot
)

logger = logging.getLogger(__name__)

logging.basicConfig(
  format = '[%(asctime)s] %(name)s : %(message)s', 
  level = logging.ERROR,
  handlers = [
    logging.FileHandler(
      os.path.join('C:\\users\\rdpuser\\Desktop\\logs', 'python.log')
    )
  ]
)

if __name__ == '__main__':

  pid_filepath = None
  node_uid = None
  cluster_uid = None

  try:
    
    with daemoniker.Daemonizer() as (is_setup, daemonizer):

      if is_setup:
        node_uid = sys.argv[1]
        cluster_uid = sys.argv[2]
        pid_filepath = create_pid_filepath(node_uid, cluster_uid)

      is_parent, node_uid, cluster_uid, pid_filepath = daemonizer(
        pid_filepath, node_uid, cluster_uid, pid_filepath
      )

      if is_parent:
        pass
    
    if platform.system() == 'Windows':
      del os.environ['__INVOKE_DAEMON__']

    while True:
      with lock('monitor'):
        running = scan_processes(cluster_uid)
      workers = [node_uid for node_uid, _ in running if node_uid != 'monitor']
      submitted = []
      for process in objects(constants.PROCESS_NAMESPACE):
        if len(process.status) == 1:
          assert process.status == ['submitted']
          submitted.append(process)
        else:
          assert process.node_uid is not None
          if process.node_uid in workers:
            workers.remove(process.node_uid)
          else:
            pass
      delta = len(workers) - len(submitted)
      if delta < 2:
        for i in range(2 - delta):
          launch_node(create_string_digest(str(uuid.uuid4())), 'parkit.process.workerdaemon', cluster_uid)
      elif delta > 5:
        for i in range(delta - 5):
          pass
      time.sleep(0)

  except Exception as e:
    log(e)
  finally:
    os._exit(0)
    



