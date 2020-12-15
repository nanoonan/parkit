from sortedcontainers import SortedDict

class Indexed():
	pass

class Hashed():
	pass

pipeline = lambda a, b, c, d: d[-1].encode(a, b, c, d[:-1]) if len(d) else (a, b, c)

class Hash():

	@staticmethod
	def encode(key, value, encoded_value, hashers):
		return pipeline(key, value, encoded_value, encoders)

class Encode():

	@staticmethod
	def encode(obj, encoders):
		return pipeline(obj, encoders)

	@staticmethod
	def decode(obj, encoders):
		return pipeline(obj, encoders)

class IndexedHashed():

	def __init__(self, hashers = [], encoders = []):
		self._key_hashers = reversed(key_hashers)
		self._value_encoders = reversed(encoders)
		self._value_decoders = encoders
		self._keyvalue = SortedDict()
		self._indexkey = SortedDict()
		self._keyindex = SortedDict()

	def __str__(self):
		return '{0}\n{1}\n{2}'.format(self._keyvalue, self._indexkey, self._keyindex)
	
	def prepend(self):
		pass

	def append(self, item, key = None, index = False):
		pass

	def deletekey(self, key):
		pass

	def deleteindex(self, index):
		pass

	def getindex(self, index):
		return self._keyvalue[self._indexkey[index]]

	def getkey(self, key):
		key, _, _ = pipeline(key, None, None, self._key_hasher)
		return self._keyvalue[key]

	def putkeyindex(self, key, index, item):
		encoded_item = pipeline(item, self._value_encoders)
		key, _, _ = pipeline(key, item, encoded_item, self._key_hashers)
		self._keyvalue[key] = item
		if index in self._indexkey:
		
		else:
			self._indexkey[index] = (key, 0)
			self._keyindex[(key, 0)] = index


