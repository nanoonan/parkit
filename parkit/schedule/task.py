import uuid

class Task():
  
  def __init__(self, name, coro, state = None):
    self._name = name
    self._coro = coro
    self._state = state
    self._id = str(uuid.uuid4())
    self._count = 0
    self._limit = None
    self._start = None
    self._end = None
    self._except_when_fn = None
    self._pending = False

  @property
  def name(self):
    return self._name

  @property
  def coro(self):
    return self._coro

  @property
  def state(self):
    return self._state

  @state.setter
  def state(self, value):
    self._state = value

  @property
  def id(self):
    return self._id

  @property
  def pending(self):
    return self._pending

  @pending.setter
  def pending(self, value):
    self._pending = value

  @property
  def count(self):
    return self._count

  @count.setter
  def count(self, value):
    self._count = value

  @property
  def limit(self):
    return self._limit

  @property
  def start(self):
    return self._start

  @property
  def end(self):
    return self._end

  @property
  def except_when_fn(self):
    return self._except_when_fn

  @property
  def is_runnable(self):
    if self._limit is None or self._count < self._limit:
      if self._except_when_fn is None or not self._except_when_fn():
        if self._is_between():
          return True
    return False

  def set_limit(self, n):
    self._limit = n
    return self

  def set_between(self, start, end):
    self._start = start
    self._end = end
    return self

  def set_except_when(self, fn):
    self._except_when_fn = fn
    return self

  def _is_between(self):
    if self._start is None or self._end is None:
      return True
    else:
      start_hour, start_minute, start_second = [int(s) for s in self._start.split(':')]
      start = (start_hour * 60 * 60) + (start_minute * 60) + start_second
      end_hour, end_minute, end_second = [int(s) for s in self._end.split(':')]
      end = (end_hour * 60 * 60) + (end_minute * 60) + end_second
      now = (datetime.datetime.now().hour * 60 * 60) + (datetime.datetime.now().minute * 60) + datetime.datetime.now().second
      return now >= start and now <= end

  async def invoke(self):
    result, self._task.state = await self._task.coro(self._task.state)
    # store result in completion queue
    # signal new completion queue event