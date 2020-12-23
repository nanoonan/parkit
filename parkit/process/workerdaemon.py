import daemoniker
import logging
import os
import parkit.constants as constants
import parkit.storage.threadlocal as thread
import sys
import threading
import time

from parkit.storage import objects
from parkit.process.processtools import create_pid_filepath
from parkit.storage import transaction

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
    
    while True:
      candidates = []
      for process in objects(namespace = constants.PROCESS_NAMESPACE):
        if len(process.status) == 1:
          candidates.append(process)
      my_process = None
      for candidate in candidates:
        with transaction(candidate):
          if len(candidate.status) == 1:
            logger.error('a')
            my_process = candidate
            my_process._put_attr('_status', my_process.status + ['running'])
            my_process._put_attr('_node_uid', node_uid)
            break
      if my_process is not None:
        break
    logger.error('Here')
    try:
      logger.error('try')
      result = exc_value = None
      target = my_process._get_attr('_target')
      logger.error(target)
      logger.error(my_process.metadata)
      logger.error(my_process.status)
      args = my_process._get_attr('_args')
      logger.error(args)
      kwargs = my_process._get_attr('_kwargs')
      logger.error(kwargs)
      result = target()
    except BaseException as e:
      logger.exception()
      exc_value = e
    finally:
      if exc_value is None:
        with transaction(my_process):
          my_process._put_attr('_status', my_process.status + ['finished'])
          my_process._put_attr('_result', result)
      else:
        with transaction(my_process):
          my_process._put_attr('_status', my_process.status + ['failed'])
          my_process._put_attr('_error', exc_value)
      if exc_value:
        raise exc_value
    while True:
      time.sleep(1)
  except Exception as e:
    log(e)
  finally:
    os._exit(0)