# pylint: disable = c-extension-no-member
import ast
import distutils.util
import functools
import gc
import importlib
import inspect
import logging
import hashlib
import os
import sys
import time
import types

from typing import (
    Any, Callable, Dict, Generator, List, Optional, Tuple
)

logger = logging.getLogger(__name__)

def compile_function(
    code: str,
    *args: str,
    glbs: Dict[str, Any]
) -> Callable[..., Any]:
    module_ast = ast.parse(code.format(*args))
    module_code = compile(module_ast, '__dynamic__', 'exec')
    function_code = [c__ for c__ in module_code.co_consts if isinstance(c__, types.CodeType)][0]
    return types.FunctionType(function_code, glbs)

def get_memory_size(target: Any) -> int:
    if isinstance(target, (type, types.ModuleType, types.FunctionType)):
        raise TypeError()
    seen_ids = set()
    size = 0
    objects = [target]
    while objects:
        need_referents = []
        for obj in objects:
            if not isinstance(obj, (type, types.ModuleType, types.FunctionType)) \
            and id(obj) not in seen_ids:
                seen_ids.add(id(obj))
                size += sys.getsizeof(obj)
                need_referents.append(obj)
        objects = gc.get_referents(*need_referents)
    return size

@functools.lru_cache(None)
def create_class(qualified_class_name: str) -> type:
    module_path, class_name = qualified_class_name.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)

def get_qualified_class_name(obj: Any) -> str:
    if isinstance(obj, type):
        return obj.__module__ + '.' + obj.__name__
    return obj.__class__.__module__ + '.' + obj.__class__.__name__

def getenv(name: str, vartype: Optional[type] = None) -> Any:
    name = str(name)
    value = os.getenv(name)
    if value is None:
        raise ValueError('Environment variable {0} not found in environment'.format(name))
    if vartype is None or vartype is str:
        return value
    if vartype is bool:
        return bool(distutils.util.strtobool(value))
    if vartype is int:
        return type(value)
    raise TypeError()

def checkenv(name: str, vartype: type) -> bool:
    if vartype is bool:
        if not getenv(name).upper() == 'FALSE' and not getenv(name).upper() == 'TRUE':
            raise TypeError('Environment variable {0} has wrong type'.format(name))
    else:
        try:
            type(getenv(name))
        except Exception as exc:
            raise TypeError('Environment variable has {0} wrong type'.format(name)) from exc
    return True

def envexists(name: str) -> bool:
    return os.getenv(str(name)) is not None

def setenv(name: str, value: str) -> None:
    os.environ[str(name)] = str(value)

@functools.lru_cache(None)
def resolve_path(path: str) -> Tuple[str, Optional[str]]:
    segments = [segment for segment in path.split('/') if len(segment)]
    if segments and \
    all(
        segment.isascii() and segment.replace('_', '').replace('-', '').isalnum()
        for segment in segments
    ):
        return \
        (segments[0], None) if len(segments) == 1 else \
        (segments[-1], '/'.join(segments[0:-1]))
    raise ValueError('Path does not follow naming rules')

@functools.lru_cache(None)
def resolve_namespace(namespace: Optional[str]) -> Optional[str]:
    if not namespace:
        return namespace
    segments = [segment for segment in namespace.split('/') if len(segment)]
    if all(
        segment.isascii() and segment.replace('_', '').replace('-', '').isalnum()
        for segment in segments
    ):
        return None if not segments else '/'.join(segments)
    raise ValueError('Namespace does not follow naming rules')

def create_string_digest(*segments: Any) -> str:
    return hashlib.sha1(
        ''.join([str(segment) for segment in segments]).encode('utf-8')
    ).hexdigest()

def polling_loop(
    interval: float,
    max_iterations: Optional[int] = None,
    initial_offset: Optional[float] = None,
    timeout: Optional[float] = None
) -> Generator[int, None, None]:
    interval = interval if interval is None or interval > 0 else 0
    max_iterations = max_iterations if max_iterations is None or max_iterations > 0 else 0
    if timeout is not None and timeout <= 0:
        raise TimeoutError()
    iteration = 0
    start_ns = None
    try:
        while True:
            if max_iterations is not None and iteration == max_iterations:
                return
            loop_start_ns = time.time_ns()
            start_ns = loop_start_ns if start_ns is None else start_ns
            yield iteration
            iteration += 1
            now = time.time_ns()
            if iteration == 1 and initial_offset is not None:
                sleep_duration = interval - ((now - loop_start_ns - initial_offset) / 1e9)
            else:
                sleep_duration = interval - ((now - loop_start_ns) / 1e9)
            if timeout is not None and now + sleep_duration - start_ns >= timeout * 1e9:
                raise TimeoutError()
            if sleep_duration > 0:
                time.sleep(sleep_duration)
    except GeneratorExit:
        pass

def get_calling_modules() -> List[str]:
    modules = []
    frame = inspect.currentframe()
    while frame:
        module = inspect.getmodule(frame)
        if module is not None:
            name = module.__name__
            package = module.__name__.split('.')[0]
            if package != 'parkit':
                modules.append(name)
        frame = frame.f_back
    return modules
