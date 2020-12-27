from typing import (
	Any, Dict, Tuple
)

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
