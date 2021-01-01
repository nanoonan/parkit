# pylint: disable = no-value-for-parameter
import logging

logger = logging.getLogger(__name__)

class EntityMeta(type):

    def __initialize_class__(cls):
        pass

    def __call__(cls, *args, **kwargs):
        print('entity meta')
        cls.__initialize_class__()
        return super().__call__(*args, **kwargs)
