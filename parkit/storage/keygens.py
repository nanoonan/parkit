import mmh3

from parkit.storage.encoder import KeyEncoder

class MMH3(KeyEncoder):

	def encode(self, obj):
		self.is_ordered = False
		self.key = mmh3.hash128(obj, MMH3_SEED, True, signed = False)
		return obj