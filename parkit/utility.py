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

logger = logging.getLogger(__name__)

def get_memory_size(target):
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
def create_class(qualified_class_name):
    module_path, class_name = qualified_class_name.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)

def get_qualified_class_name(obj):
    if isinstance(obj, type):
        return obj.__module__ + '.' + obj.__name__
    return obj.__class__.__module__ + '.' + obj.__class__.__name__

@functools.lru_cache(None)
def getenv(name, vartype = None):
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

def checkenv(name, vartype):
    if vartype is bool:
        if not getenv(name).upper() == 'FALSE' and not getenv(name).upper() == 'TRUE':
            raise TypeError('Environment variable {0} has wrong type'.format(name))
    else:
        try:
            type(getenv(name))
        except Exception as exc:
            raise TypeError('Environment variable has {0} wrong type'.format(name)) from exc
    return True

def envexists(name):
    return os.getenv(str(name)) is not None

def setenv(name, value):
    os.environ[str(name)] = str(value)

@functools.lru_cache(None)
def resolve(obj: str, path: bool = True):
    segments = [segment for segment in obj.split('/') if len(segment)]
    if not path:
        if all([
            segment.isascii() and segment.replace('_', '').replace('-', '').isalnum()
            for segment in segments
        ]):
            return None if not segments else '/'.join(segments)
        raise ValueError('Namespace does not follow naming rules')
    if segments and \
    all([
        segment.isascii() and segment.replace('_', '').replace('-', '').isalnum()
        for segment in segments
    ]):
        return (
            segments[0], None) if len(segments) == 1 \
            else (segments[-1], '/'.join(segments[0:-1])
        )
    raise ValueError('Path does not follow naming rules')

def create_string_digest(*segments):
    return hashlib.sha1(''.join([str(segment) for segment in segments]).encode('utf-8')).hexdigest()
    # return mmh3.hash128(encoded_value, MMH3Hasher.MMH3_SEED, True, signed = False)

def polling_loop(interval, max_iterations = None, initial_offset = None):
    iteration = 0
    try:
        while True:
            start_ns = time.time_ns()
            yield iteration
            iteration += 1
            if max_iterations is not None and iteration == max_iterations:
                return
            if iteration == 1 and initial_offset is not None:
                sleep_duration = interval - ((time.time_ns() - start_ns - initial_offset) / 1e9)
            else:
                sleep_duration = interval - ((time.time_ns() - start_ns) / 1e9)
            if sleep_duration > 0:
                time.sleep(sleep_duration)
    except GeneratorExit:
        pass

def get_calling_modules():
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

class HighResolutionTimer():

    def __init__(self):
        self.start_ns = None

    def start(self):
        self.start_ns = time.time_ns()

    def stop(self):
        elapsed = time.time_ns() - self.start_ns
        print('elapsed: {0} ms'.format(elapsed / 1e6))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.stop()
        if exc_value is not None:
            raise exc_value
