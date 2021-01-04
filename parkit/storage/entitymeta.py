import logging

from typing import (
    Any, Dict
)

logger = logging.getLogger(__name__)

class EntityMeta(type):

    def __initialize_class__(cls) -> None:
        if not hasattr(cls, '_Entity__def'):
            setattr(cls, '_Entity__def', dir(cls))

    def __call__(cls, *args: Any, **kwargs: Dict[str, Any]) -> Any:
        obj = super().__call__(*args, **kwargs)
        cls.__initialize_class__()
        return obj