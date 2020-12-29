# pylint: disable = c-extension-no-member, broad-except, protected-access
import logging

logger = logging.getLogger(__name__)

class ObjectMeta(type):

    def __initialize_class__(cls):
        pass

    def __call__(cls, *args, **kwargs):
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)
