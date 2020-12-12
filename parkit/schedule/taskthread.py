import asyncio
import logging
import concurrent.futures
import threading
import time

from parkit.asyncthread import (
  AsyncThread,
  AsyncThreadState
)

logger = logging.getLogger(__name__)

class TaskThread(AsyncThread):

  def __init__(self, task):
    super().__init__()
    self._task = task
    self._queue = asyncio.Queue()
    
  async def _set_event(self):
    await self._queue.put(True)
  
  async def _task_runner(self):
    logger.error('task runner started')
    while True:
      try:
        await self._queue.get()
        asyncio.create_task(self._task.invoke())
      except:
        logger.exception('')

  def set(self):
    assert self.state == AsyncThreadState.Started
    asyncio.run_coroutine_threadsafe(self._set_event(), self._loop)

  def start(self):
    super().start()
    asyncio.run_coroutine_threadsafe(self._task_runner(), self._loop)

    

    