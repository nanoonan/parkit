# pylint: disable = too-few-public-methods
import ast
import distutils.util
import functools
import gc
import hashlib
import importlib
import inspect
import logging
import os
import platform
import sys
import time
import types
import uuid

from typing import (
    Any, Callable, Dict, Iterator, List, Optional, Set, Tuple, Union
)

import parkit.constants as constants

if platform.system() == 'Windows':
    from ctypes import Structure, byref, windll
    from ctypes.wintypes import WORD, DWORD, LPVOID

if platform.system() == 'Unix':
    import resource

logger = logging.getLogger(__name__)

class Timer():

    def __init__(self, name: str):
        self._start = None
        self._name = name

    def start(self):
        self._start = time.time_ns()

    def stop(self):
        print(
            'timer for %s: %f ms', self._name,
            (time.time_ns() - self._start) / 1e6
        )

def compile_function(
    code: str,
    /, *,
    glbs: Dict[str, Any],
    name: Optional[str] = None,
    defaults: Tuple[Any, ...] = ()
) -> Callable[..., Any]:
    module_ast = ast.parse(code)
    module_code = compile(module_ast, '__dynamic__', 'exec')
    function_code = [c__ for c__ in module_code.co_consts if isinstance(c__, types.CodeType)][0]
    if name is None:
        name = str(uuid.uuid4())
    return types.FunctionType(function_code, glbs, name, defaults)

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
def create_class(name: str) -> type:
    module_path, class_name = name.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)

def get_qualified_class_name(obj: Any) -> str:
    if isinstance(obj, type):
        return obj.__module__ + '.' + obj.__name__
    return obj.__class__.__module__ + '.' + obj.__class__.__name__

def get_qualified_base_names(
    obj: Any,
    names: Optional[Set[str]] = None
) -> Set[str]:
    names = set() if names is None else names
    if isinstance(obj, type):
        if obj == object:
            return set()
        names.add(obj.__module__ + '.' + obj.__name__)
    else:
        names.add(obj.__class__.__module__ + '.' + obj.__class__.__name__)
    if isinstance(obj, type):
        bases = obj.__bases__
    else:
        bases = obj.__class__.__bases__
    for base in bases:
        get_qualified_base_names(base, names)
    return names

def getenv(name: str, vartype: type = str) -> Any:
    value = os.getenv(name)
    assert value is not None
    if vartype is str:
        return value
    if vartype in [int, float]:
        return vartype(value)
    if vartype is bool:
        return bool(distutils.util.strtobool(value))
    raise TypeError()

def checkenv(name: str, vartype: type) -> bool:
    if vartype is bool:
        if getenv(name, str).upper() != 'FALSE' and getenv(name, str).upper() != 'TRUE':
            raise TypeError()
    else:
        try:
            type(getenv(name, str))
        except Exception as exc:
            raise TypeError() from exc
    return True

def envexists(name: str) -> bool:
    return name in os.environ

def setenv(name: str, value: Optional[Any]):
    if value is None:
        if name in os.environ:
            del os.environ[name]
        return
    os.environ[name] = str(value)

def resolve_name(name: str) -> str:
    if not name:
        raise ValueError()
    if name.isascii() and \
    name.replace('.', '').replace('_', '').replace('-', '').isalnum():
        return name
    raise ValueError()

def resolve_path(path: Optional[str]) -> Tuple[str, str]:
    if not path:
        return (
            constants.DEFAULT_NAMESPACE,
            ''.join(['__', str(uuid.uuid4()), '__'])
        )
    if path == constants.MEMORY_NAMESPACE:
        return (
            constants.MEMORY_NAMESPACE,
            ''.join(['__', str(uuid.uuid4()), '__'])
        )
    segments = [segment for segment in path.split('/') if len(segment)]
    if segments and \
    all(
        segment.isascii() and \
        segment.replace('_', '').replace('-', '').isalnum()
        for segment in segments[:-1]
    ):
        if segments[-1].isascii() and \
        segments[-1].replace('.', '').replace('_', '').replace('-', '').isalnum():
            return \
            (constants.DEFAULT_NAMESPACE, segments[0]) if len(segments) == 1 else \
            ('/'.join(segments[0:-1]), segments[-1])
    raise ValueError()

def resolve_namespace(namespace: Optional[str]) -> str:
    if not namespace:
        return constants.DEFAULT_NAMESPACE
    segments = [segment for segment in namespace.split('/') if len(segment)]
    if all(
        segment.isascii() and segment.replace('_', '').replace('-', '').isalnum()
        for segment in segments
    ):
        return '/'.join(segments)
    raise ValueError()

def create_string_digest(obj: Union[str, bytes]) -> str:
    if isinstance(obj, str):
        obj = obj.encode()
    return hashlib.sha1(obj).hexdigest()

def polling_loop(
    interval: float,
    max_iterations: Optional[int] = None,
    initial_offset: Optional[float] = None,
    timeout: Optional[float] = None
) -> Iterator[int]:
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
            modules.append(name)
        frame = frame.f_back
    return modules

def get_pagesize():
    if platform.system() == 'Windows':
        class SystemInfo(Structure):
            _fields_ = [
                ("wProcessorArchitecture", WORD),
                ("wReserved", WORD),
                ("dwPageSize", DWORD),
                ("lpMinimumApplicationAddress", LPVOID),
                ("lpMaximumApplicationAddress", LPVOID),
                ("dwActiveProcessorMask", DWORD),
                ("dwNumberOfProcessors", DWORD),
                ("dwProcessorType", DWORD),
                ("dwAllocationGranularity", DWORD),
                ("wProcessorLevel", WORD),
                ("wProcessorRevision", WORD),
            ]
        system_info = SystemInfo()
        windll.kernel32.GetSystemInfo(byref(system_info))
        return system_info.dwPageSize
    if platform.system() == 'Unix':
        return resource.getpagesize()
    raise NotImplementedError()
