import logging

logger = logging.getLogger(__name__)

class EntityMeta(type):

    def __initialize_class__(cls):
        pass

    def __call__(cls, *args, **kwargs):
        return super().__call__(*args, **kwargs)
