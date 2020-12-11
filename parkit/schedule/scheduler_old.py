import asyncio
import cloudpickle
import datetime
import importlib
import logging
import os
import psutil
import schedule
import subprocess
import tempfile
import uuid

from parcolls.constants import *
from parcolls.daemonclean import (
  cleanup_daemon_tree,
  terminate_daemon
)
from parcolls.task import Task
from parcolls.utility import create_key
from parcolls.workspace import (
  KeyPrefixDict,
  Workspace
)

from daemoniker import (
  send,
  SIGINT
)

logger = logging.getLogger(__name__)

class TaskScheduler():

  def __init__(self, coro, state):
    self._name = None
    self._coro = coro
    self._state = state
    self._scheduling = list()
    self._once = False

  @property
  def name(self):
    return self._name

  @name.setter
  def name(self, value):
    self._name = value

  @property
  def coro(self):
    return self._coro

  @property
  def state(self):
    return self._state

  @property
  def day(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.day))
    return self

  @property
  def days(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.days))
    return self

  @property
  def week(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.week))
    return self

  @property
  def monday(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.monday))
    return self

  @property
  def tuesday(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.tuesday))
    return self

  @property
  def wednesday(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.wednesday))
    return self

  @property
  def thursday(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.thursday))
    return self

  @property
  def friday(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.friday))
    return self

  @property
  def saturday(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.saturday))
    return self

  @property
  def sunday(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.sunday))
    return self

  @property
  def weeks(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.weeks))
    return self

  @property
  def second(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.second))
    return self

  @property
  def seconds(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.seconds))
    return self

  @property
  def minute(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.minute))
    return self

  @property
  def minutes(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.minutes))
    return self

  @property
  def hour(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.hour))
    return self

  @property
  def hours(self):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.hours))
    return self

  def at(self, time):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.at(time)))
    return self

  def to(self, latest):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.to(latest)))
    return self

  def n_times(self, n = 1):
    self._scheduling.append(lambda task, scheduler: (task.set_limit(n), scheduler))
    return self

  def except_when(self, fn = lambda: False):
    self._scheduling.append(lambda task, scheduler: (task.set_except_when(fn), scheduler))
    return self
    
  def between(self, start, end):
    self._scheduling.append(lambda task, scheduler: (task.set_betweeb(start, end), scheduler))
    return self
    
  def once(self):
    self._once = True
    self._scheduling.append(lambda task, scheduler: (task, scheduler.every(1).second))
    return self

  def every(self, interval = 1):
    self._scheduling.append(lambda task, scheduler: (task, scheduler.every(interval)))
    return self

class Job():

  __cloudpickle__ = True

  def __init__(self, name, task_schedulers):
    
    self._name = str(name)
    self._scheduler = schedule.Scheduler()
    self._id = str(uuid.uuid4())
    self._status = 'Pending Startup'
    self._tasks = list()

    def set_pending(task, once):
      def f():
        task.pending = True
        if once:
          return schedule.CancelJob
      return f

    for task_scheduler in task_schedulers:
      task = Task(self, task_scheduler._coro, task_scheduler._state)
      scheduler = self._scheduler
      for action in task_scheduler._scheduling:
        task, scheduler = action(task, scheduler)
      scheduler.do(set_pending(task, task_scheduler._once))
      self._tasks.append(task)

  @property
  def tasks(self):
    return self._tasks

  @property
  def pending(self):
    self._scheduler.run_pending()
    runnable = []
    for task in [task for task in self._tasks if task.pending]:
      task.pending = False
      if task.is_runnable:
        task.count += 1
        runnable.append(task)
    return runnable

  @property
  def idle_seconds(self):
    return self._scheduler.idle_seconds

  @property
  def has_scheduled_tasks(self):
    return len(self._scheduler.jobs) > 0

  @property
  def name(self):
    return self._name

  @property
  def id(self):
    return self._id

  @property
  def status(self):
    return self._status

  @status.setter
  def status(self, value):
    self._status = value

  async def marshal(self):
    for task in self._tasks:
      await task.marshal()

def create_task(task_scheduler, name):
  task_scheduler.name = name
  # write to database
  # publish message

def schedule(coro, initial_state = None):
  return TaskScheduler(coro, initial_state)

def delete_task(name):
  pass

def list_tasks():
  pass

#
#
#

class Scheduler():

  async def schedule(self, *task_schedulers, job_name = '<anonymous>'):

    if len(task_schedulers) == 1 and isinstance(task_schedulers[0], list): 
      task_schedulers = task_schedulers[0]
  
    job = Job(job_name, task_schedulers)

    await job.marshal()

    await _start_job(job)

  async def list(self):
    return await _list_jobs()

  async def delete(self, *values):
    return await _stop_jobs(values) 

async def _stop_jobs(*values):

  def status_updater(key, value):
    if value and value.status == 'Running':
      value.status = 'Pending Shutdown'
      return (True, value)
    else:
      return False

  shared_dict = KeyPrefixDict(Workspace(SYSTEM_WORKSPACE_NAME).dict, 'daemon')
  job_keys = []

  if len(values) == 1 and isinstance(values[0], list):
    values = values[0]

  for value in values:
    if isinstance(value, Job):
      job_keys.append(create_key(value.id, 'job'))
    else:
      async for key in shared_dict.keys():
        if key.split('/')[0] == value:
          job_keys.append(key)
          break
        try:
          job = await shared_dict.get(key)
          if job.name == value:
            job_keys.append(key)
        except KeyError:
          pass

  if len(job_keys) == 0:
    return False

  results = []
  for job_key in job_keys:
    updated, job = await shared_dict.update(job_key, status_updater)
    if updated:
      pidfile = get_daemon_pidfile(job.id)
      send(pidfile, SIGINT)
      results.append(True)
    else:
      results.append(False)
    
  results if len(results) > 1 else results[0]

async def _list_jobs():
  
  shared_dict = KeyPrefixDict(Workspace(SYSTEM_WORKSPACE_NAME).dict, 'daemon')

  terminated_daemon_ids, running_daemon_ids = cleanup_daemon_tree()
  
  #
  # Remove from system database recorded daemons that are no longer running.
  #
  for daemon_id in terminated_daemon_ids:
    await shared_dict.delete(daemon_id + '%')
    
  #
  # Delete from system database records that don't correspond to running daemons.
  #
  daemon_ids = set()
  abandoned_daemon_ids = set()
  async for key in shared_dict.keys():
    daemon_id = key.split('/')[0]
    if daemon_id not in running_daemon_ids:
      abandoned_daemon_ids.add(daemon_id)
    else:
      daemon_ids.add(daemon_id)
  for daemon_id in abandoned_daemon_ids:
    await shared_dict.delete(daemon_id + '%')

  jobs = []
  for daemon_id in daemon_ids:
    try:
      jobs.append(await shared_dict.get(create_key(daemon_id, 'job')))
    except KeyError:
      pass
  
  return jobs
        
async def _start_job(job, timeout = DEFAULT_DAEMON_STARTUP_TIMEOUT):

  def status_updater(key, value):
    if value and value.status == 'Pending Startup':
      return True
    else:
      return False  

  try:
    
    shared_dict = KeyPrefixDict(Workspace(SYSTEM_WORKSPACE_NAME).dict, 'daemon')
    
    job_key = create_key(job.id, 'job')

    await shared_dict.set(job_key, job)

    module = importlib.import_module('parlib.daemon')
    path = os.path.abspath(module.__file__)
    subprocess.run(['python', path, job.id, 'PARLIBDAEMON'], check = True)

    start = asyncio.get_running_loop().time()
    while True:
      # FIXME: wait for pidfile created
      try:
        status = (await shared_dict.get(job_key)).status
      except KeyError:
        raise Exception()
      if status == 'Running':
        break
      elif status != 'Pending Startup':
        raise Exception()
      now = asyncio.get_running_loop().time()
      if now - start > timeout:
        raise Exception()
      await asyncio.sleep(DAEMON_STARTUP_POLLING_INTERVAL)
  
  except:

    (updated, job) = await shared_dict.update(job_key, status_updater)
    if updated or (not updated and (not job or job.status != 'Running')):
      raise Exception('job startup failed')
  
