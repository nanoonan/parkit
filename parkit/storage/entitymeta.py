# pylint: disable = no-value-for-parameter
import logging

from typing import (
    Any, Dict
)

logger = logging.getLogger(__name__)

class EntityMeta(type):

    def __initialize_class__(cls: Any):
        if not hasattr(cls, '_Entity__def'):
            setattr(cls, '_Entity__def', set(dir(cls)))

    def __call__(
        cls: Any,
        *args: Any,
        **kwargs: Dict[str, Any]
    ) -> Any:
        cls.__initialize_class__()
        obj = super().__call__(*args, **kwargs)
        return obj
