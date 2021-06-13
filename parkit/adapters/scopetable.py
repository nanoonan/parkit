# pylint: disable = protected-access
import logging

from typing import (
    Any, Dict, List, Optional, Set, Tuple
)

import parkit.constants as constants

from parkit.storage.context import transaction_context
from parkit.storage.entity import Entity
from parkit.storage.site import get_site_name

logger = logging.getLogger(__name__)

scope_tables: Dict[str, Entity] = {}

def get_scope_table(site_uuid: str) -> Any:
    if site_uuid not in scope_tables:
        entity = Entity(
            constants.EXECUTION_NAMESPACE, constants.SCOPE_TABLE_NAME,
            db_properties = [{}],
            site = get_site_name(site_uuid)
        )
        scope_tables[site_uuid] = entity
        return entity
    return scope_tables[site_uuid]

def add_scope_entry(
    obj_uuid: str,
    site_uuid: str,
    process_uuid: Optional[str] = None
):
    scope_table = get_scope_table(site_uuid)
    with transaction_context(scope_table._Entity__env, write = True) as (txn, _, _):
        key = obj_uuid.encode('utf-8')
        value = process_uuid.encode('utf-8') if process_uuid else ''.encode('utf-8')
        assert txn.put(
            key = key, value = value, overwrite = True, append = False,
            db = scope_table._Entity__userdb[0]
        )

def dump_scope_table(site_uuid: str) -> List[Tuple[str, str]]:
    scope_table = get_scope_table(site_uuid)
    results = []
    with transaction_context(scope_table._Entity__env, write = False) as (_, cursors, _):
        cursor = cursors[scope_table._Entity__userdb[0]]
        if cursor.first():
            while True:
                key = cursor.key()
                key = bytes(key) if isinstance(key, memoryview) else key
                value = cursor.value()
                value = bytes(value) if isinstance(value, memoryview) else value
                results.append((key.decode('utf-8'), value.decode('utf-8')))
                if not cursor.next():
                    break
    return results

def check_scope(
    obj_uuid: str,
    site_uuid: str,
    active_scopes: Set[str]
) -> bool:
    scope_table = get_scope_table(site_uuid)
    with transaction_context(scope_table._Entity__env, write = True) as (_, cursors, _):
        recorded_scopes = set()
        cursor = cursors[scope_table._Entity__userdb[0]]
        target = obj_uuid.encode('utf-8')
        if cursor.set_range(target):
            if cursor.key() == target:
                while True:
                    value = cursor.value()
                    value = bytes(value) if isinstance(value, memoryview) else value
                    value = value.decode('utf-8')
                    if not value:
                        return True
                    recorded_scopes.add(value)
                    if not cursor.next():
                        break
                    if not cursor.key() == target:
                        break
                if not bool(
                    {scope for scope in recorded_scopes if scope}.intersection(active_scopes)
                ):
                    cursor.set_range(target)
                    cursor.delete(dupdata = True)
                else:
                    return True
    return False
