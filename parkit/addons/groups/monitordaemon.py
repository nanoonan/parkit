import daemoniker
import logging
import os
import platform
import psutil
import sys
import time

from parkit.adapters import (
  Dict,
  WriteTransaction
)
from parkit.exceptions import *
from parkit.utility import *
from parkit.addons.groups.clustertools import (
  create_pid_filepath,
  launch_node,
  scan_processes,
  terminate_node
)
from parkit.addons.groups.constants import *

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
  cluster_id = None

  try:
    
    with daemoniker.Daemonizer() as (is_setup, daemonizer):

      if is_setup:
        if platform.system() == 'Windows':
          node_uid = sys.argv[1].split('=')[1] + '-aux'
        else:
          node_uid = sys.argv[1].split('=')[1]
        cluster_id = sys.argv[2]
        pid_filepath = create_pid_filepath(node_uid, cluster_id)

      is_parent, node_uid, cluster_id, pid_filepath = daemonizer(
        pid_filepath, node_uid, cluster_id, pid_filepath
      )

      if is_parent:
        pass

    signal_handler = daemoniker.SignalHandler1(pid_filepath)
    signal_handler.start()

    # settings = Dict(SETTINGS_DATABASE, context = SYSTEM_CONTEXT, policy = [DictPolicy.AllowRead])

    if platform.system() == 'Windows':
      try:
        node_uid = sys.argv[1].split('=')[1]
        pid_filepath = create_pid_filepath(node_uid, cluster_id)
        with open(pid_filepath, 'wt') as file:
          file.write(str(os.getpid()))
      except Exception as e:
        log(e)
        sys.exit(1)
    
    if platform.system() == 'Windows':
      del os.environ['__INVOKE_DAEMON__']

    processes = Dict.create_or_bind(PROCESSES_NAME, namespace = GROUPS_NAMESPACE)
    with WriteTransaction(processes) as processes:
      if 'monitor' not in processes:
        processes['monitor'] = node_uid
      else:
        if not process_tag_exists(node_uid):
          processes['monitor'] = node_uid
        else:
          raise Exception()

    while True:
      time.sleep(1)
    # n_task_nodes = settings['cluster_size']
    # for _ in polling_loop(settings['monitor_polling_interval']):
    #   try:
    #     with Lock('__monitor_lock__', mode = LockMode.Writer):
    #       running_processes = scan_processes()
    #       running_task_nodes = [(node_uid, cmdline) for (node_uid, cmdline) in running_processes if 'monitordaemon' not in cmdline[1]]
    #       task_nodes = {}
    #       for (node_uid, cmdline) in running_task_nodes:
    #         task_nodes[int(node_uid.split('-')[0])] = node_uid
    #       for node_id in range(n_task_nodes):
    #         if node_id not in task_nodes:
    #           launch_node('{0}-{1}'.format(node_id, str(uuid.uuid4())), 'pinky.cluster.taskdaemon')
    #       for node_id in range(len(task_nodes) - 1, n_task_nodes - 1, -1):
    #         terminate_node(task_nodes[node_id])
    #   except daemoniker.exceptions.SIGINT:
    #     raise
    #   except Exception as e:
    #     log(e)

  except Exception as e:
    log(e)
  finally:
    sys.exit(1)
    



