# pylint: disable = no-value-for-parameter
#
# reviewed: 6/16/21
#
import logging

from typing import (
    Any, Dict, Tuple
)

logger = logging.getLogger(__name__)

initialized = set()

class Missing():

    def __call__(
        self,
        *args: Tuple[Any, ...],
        **kwargs: Dict[str, Any]
    ) -> Any:
        raise NotImplementedError()

    @property
    def missing(self):
        return True

class EntityMeta(type):

    def __initialize_class__(cls: Any):
        if str(cls) not in initialized:
            setattr(cls, '_Entity__def', set(dir(cls)))
            initialized.add(str(cls))

    def __call__(
        cls: Any,
        *args: Any,
        **kwargs: Dict[str, Any]
    ) -> Any:
        cls.__initialize_class__()
        obj = super().__call__(*args, **kwargs)
        return obj

built = set()

class ClassBuilder(EntityMeta):

    def __initialize_class__(cls):
        if cls.__name__ not in built:
            targets = [cls]
            targets.extend(cls.__bases__)
            for base in targets:
                attrs = [attr for attr in dir(base) if isinstance(getattr(base, attr), Missing)]
                if len(attrs) > 0:
                    for attr in attrs:
                        cls.__build_class__(base, attr)
            built.add(cls.__name__)
        super().__initialize_class__()
