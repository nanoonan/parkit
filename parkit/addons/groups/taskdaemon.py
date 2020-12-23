import daemoniker
import logging
import os
import platform
import sys
import tempfile
import time

from pinky.cluster.asyncthread import AsyncThread
from pinky.constants import *
from pinky.databases import *
from pinky.utility import *

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

  try:
    
    with daemoniker.Daemonizer() as (is_setup, daemonizer):

      if is_setup:
        if platform.system() == 'Windows':
          node_uid = sys.argv[1].split('=')[1] + '-aux'
        else:
          node_uid = sys.argv[1].split('=')[1]
        pid_filepath = create_pid_filepath(node_uid)
        
      is_parent, node_uid, pid_filepath = daemonizer(
        pid_filepath, node_uid, pid_filepath
      )

      if is_parent:
        pass

    signal_handler = daemoniker.SignalHandler1(pid_filepath)
    signal_handler.start()
    
    if platform.system() == 'Windows':
      try:
        node_uid = sys.argv[1].split('=')[1]
        pid_filepath = create_pid_filepath(node_uid)
        with open(pid_filepath, 'wt') as file:
          file.write(str(os.getpid()))
      except Exception as e:
        log(e)
        sys.exit(1)

    while True:
      time.sleep(1)

  except Exception as e:
    log(e)
  finally:
    sys.exit(1)
    
    # task_threads = {}

    # scheduler = schedule.Scheduler()

    # while True:
    #   time.sleep(1)

    # task_dict = KeyPrefixDict(Workspace(SYSTEM_WORKSPACE_NAME).dict, 'task')

    # while True:
    #   while (message, task_id):
    #     if action == 'delete':
    #       scheduler.clear(task_id)
    #       task_thread[task_id].stop()
    #       del task_threads[task_id]
    #     elif action == 'insert':
    #       def set_pending(task, once):
    #         def f():
    #           task.pending = True
    #           if once:
    #             return schedule.CancelJob
    #         return f

    #       task_scheduler = task_dict.get(create_key(task_id, 'task_scheduler'))
    #       task = Task(task_scheduler.name, task_scheduler.coro, task_scheduler.state)
    #       for action in task_scheduler._scheduling:
    #         task, scheduler = action(task, scheduler)
    #       scheduler.do(set_pending(task, task_scheduler._once)).tag(task_id)
    #       task_threads[task.id] = TaskThread(task)
    #       task_threads[task.id].start()

    #   if len(scheduler.jobs) > 0 and scheduler.idle_seconds <= 0:
    #     scheduler.run_pending()
    #     runnable = []
    #     for task in [task for task in self._tasks if task.pending]:
    #       task.pending = False
    #       if task.is_runnable:
    #         task.count += 1
    #       runnable.append(task)
    #       for task in runnable:
    #         task_threads[task.id].set()
    #   else:
    #     time.sleep(1)

    # async def get_job(daemon_id):
    #   logger.error('booyup')
    #   shared_dict = KeyPrefixDict(Workspace(SYSTEM_WORKSPACE_NAME).dict, 'daemon')
    #   return await shared_dict.get(create_key(daemon_id, 'job'))

    # logger.error('HERE2')

    # try:
    #   async_thread = AsyncThread()
    #   async_thread.start()
    #   job = async_thread.run_task(get_job(daemon_id))
    # finally:
    #   async_thread.stop()
    
    # logger.error('HERE3')
    # for task in job.tasks:
    #   task_threads[task.id] = TaskThread(task)
    #   task_threads[task.id].start()

    # while job.has_scheduled_tasks:
    #   if job.idle_seconds > 0:
    #     time.sleep(job.idle_seconds)
    #   else:
    #     pending = job.pending
    #     for task in pending:
    #       task_threads[task.id].set()

  # except:
  #   logger.exception('')
  # finally:
  #   for thread in task_threads:
  #     thread.stop()
    
  
