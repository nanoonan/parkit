from abc import ABCMeta

class Clock(metaclass = ABCMeta):
	
	def __init__(self, code, *args, **kwargs):
		self._code = code
		self._args = args
		self._kwargs = kwargs
		self._uuid = uuid

	@property
	def uuid(self):
		return self._uuid

	def tick(self, tick):
		pass

class OneShot(Clock):
	pass
	
class ClockInstance(Clock):
	pass