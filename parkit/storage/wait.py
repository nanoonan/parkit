#
# reviewed: 6/16/21
#
import logging
import types

import parkit.constants as constants

from parkit.storage.context import transaction_context
from parkit.storage.entity import Entity
from parkit.storage.environment import get_environment_threadsafe

from parkit.utility import (
    getenv,
    polling_loop
)

logger = logging.getLogger(__name__)

def wait(*args):
    args = list(args)
    if not args:
        raise ValueError()
    if isinstance(args[-1], types.FunctionType):
        condition = args.pop()
    else:
        condition = lambda: True
    if not all(isinstance(arg, Entity) for arg in args):
        raise ValueError()
    if len(args) > 0:
        if [
            (arg.site_uuid, arg.namespace)
            for arg in args
        ].count((args[0].site_uuid, args[0].namespace)) != len(args):
            raise ValueError()
    versions = []
    for _ in polling_loop(getenv(constants.ADAPTER_POLLING_INTERVAL_ENVNAME, float)):
        if len(args) > 0:
            _, env, _, _, _, _ = get_environment_threadsafe(
                args[0].storage_path, args[0].namespace, create = False
            )
            with transaction_context(env, write = False):
                if not versions:
                    versions = [arg.version for arg in args]
                    if condition():
                        break
                else:
                    if any(
                        versions[i] != arg.version
                        for i, arg in enumerate(args)
                    ):
                        if condition():
                            break
                        versions = [arg.version for arg in args]
        else:
            if condition():
                break
