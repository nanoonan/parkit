import logging

from typing import (
    Any, Dict, Optional, Tuple
)

from parkit.adapters.object import Object

logger = logging.getLogger(__name__)

class Arguments(Object):

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        site: Optional[str] = None
    ):

        def on_init(create: bool):
            if create:
                self.args = args if args is not None else ()
                self.kwargs = kwargs if kwargs is not None else {}

        super().__init__(path, versioned = False, on_init = on_init, site = site)
