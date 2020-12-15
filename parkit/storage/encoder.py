
class Encoder():

  def encode(self, obj):
    return obj

  def decode(self, obj):
    return obj

  def set_runtime_kwargs(self, **kwargs):
    pass

  def __getstate__(self):
    return None

  def __setstate__(self, from_wire):
  	pass

class KeyEncoder(Encoder):

	def __init__(self):
		self._key = None
		self._is_ordered = False

	@property
	def is_ordered(self):
		return self._is_ordered

	@is_ordered.setter
	def is_ordered(self, value):
		self._is_ordered = value

	@property
	def key(self):
		return self._key

	@key.setter
	def key(self, key):
		self._key = key