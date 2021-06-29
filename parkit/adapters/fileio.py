#
# reviewed:
#
import codecs
import io
import logging
import mmap

from typing import (
    Any, ByteString, Callable, Dict, Iterator, List,
    Optional, Tuple, Union
)

import parkit.storage.threadlocal as thread

from parkit.adapters.object import Object
from parkit.storage.context import transaction_context

logger = logging.getLogger(__name__)

valid_modes = [
    'r', 'w', 'a', '+r', '+w', '+a',
    'br', 'bw', 'ab', '+br', '+bw', '+ab',
    'rt', 'tw', 'at', '+rt', '+tw', '+at'
]

class FileIO(Object):

    _encoding: str = 'utf-8'
    _pos: int = 0
    _extent: int = 0
    _buffer: Optional[Union[mmap.mmap, memoryview, io.StringIO]] = None
    _closed: bool = True
    _sorted_mode: str = 'br'
    _bufsize: int = 2147483648

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        create: bool = False,
        bind: bool = True,
        mode: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        site_uuid: Optional[str] = None,
        on_init: Optional[Callable[[bool], None]] = None,
        bufsize: Optional[int] = None
    ):
        self._size: int
        self._content_binary: memoryview

        def _on_init(created: bool):
            if created:
                self._size = 0
                self._content_binary = memoryview(b'')
                self._bufsize = bufsize if bufsize else 2147483648
            if mode:
                sorted_mode = ''.join(sorted(mode))
                if sorted_mode.replace('x', '') not in valid_modes:
                    raise ValueError()
                self._sorted_mode = sorted_mode
            if bufsize:
                self._bufsize = bufsize
            if on_init:
                on_init(create)

        super().__init__(
            path, metadata = metadata, site_uuid = site_uuid,
            on_init = _on_init, create = create, bind = bind
        )

    def __getstate__(self) -> Any:
        return (super().__getstate__(), self._sorted_mode, self._bufsize)

    def __setstate__(self, from_wire: Any):
        super().__setstate__(from_wire[0])
        self._sorted_mode = from_wire[1]
        self._bufsize = from_wire[2]

    def __iter__(self) -> Iterator[Union[str, ByteString]]:
        while True:
            line = self.readline()
            if len(line) == 0:
                return
            yield line

    @property
    def mode(self) -> str:
        return self._sorted_mode

    @mode.setter
    def mode(self, value: str):
        if self._closed:
            value = ''.join(sorted(value))
            if value.replace('x', '') not in valid_modes:
                raise ValueError()
            self._sorted_mode = value
        else:
            raise ValueError()

    @property
    def size(self) -> int:
        return self._size

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self):
        if 'x' in self._sorted_mode:
            thread.local.context.push(self._env, True, False)
        try:
            self._closed = False
            if 'w' not in self._sorted_mode:
                self._load_buffer(binary = 'b' in self._sorted_mode)
            else:
                self._buffer = mmap.mmap(-1, self._bufsize) \
                if 'b' in self._sorted_mode else io.StringIO(newline = None)
            return self
        except BaseException as exc:
            if self._buffer is not None:
                del self._buffer
            self._pos = 0
            self._extent = 0
            self._buffer = None
            self._closed = True
            raise exc

    def __exit__(self, error_type: type, error: Optional[Any], traceback: Any):
        try:
            if self.writable():
                assert isinstance(self._buffer, (mmap.mmap, io.StringIO))
                self._save_buffer()
        finally:
            if self._buffer is not None:
                del self._buffer
            self._pos = 0
            self._extent = 0
            self._buffer = None
            self._closed = True
            if 'x' in self._sorted_mode:
                thread.local.context.pop(self._env, abort = error is not None)

    @property
    def encoding(self) -> str:
        return self._encoding

    @staticmethod
    def isatty() -> bool:
        return False

    def readable(self) -> bool:
        if '+' in self._sorted_mode or 'r' in self._sorted_mode:
            return True
        return False

    def writable(self) -> bool:
        if 'a' in self._sorted_mode or 'w' in self._sorted_mode:
            return True
        return False

    @staticmethod
    def seekable() -> bool:
        return True

    @staticmethod
    def close():
        raise io.UnsupportedOperation()

    def tell(self) -> int:
        if not self._closed:
            if isinstance(self._buffer, memoryview):
                assert self._pos >= 0
                return self._pos
            assert self._buffer is not None
            return self._buffer.tell()
        raise ValueError()

    def seek(self, offset: int, whence: int = 0):
        if not self._closed:
            if isinstance(self._buffer, memoryview):
                pos = self._pos
                if whence == 0:
                    pos = offset
                elif whence == 1:
                    pos += offset
                elif whence == 2:
                    pos = self._size + offset
                if pos >= 0:
                    self._pos = pos
            else:
                assert self._buffer is not None
                if isinstance(self._buffer, mmap.mmap):
                    if whence == 2:
                        self._buffer.seek(self._extent, 0)
                        self._buffer.seek(offset, 1)
                        return
                assert isinstance(self._buffer, (mmap.mmap, io.StringIO))
                self._buffer.seek(offset, whence)
            return
        raise ValueError()

    def readline(self, size: int = -1) -> Union[str, bytes, memoryview]:
        if self.readable() and not self._closed:
            if isinstance(self._buffer, memoryview):
                raise io.UnsupportedOperation()
            if isinstance(self._buffer, mmap.mmap):
                return self._buffer.readline()
            assert isinstance(self._buffer, io.StringIO)
            return self._buffer.readline(size)
        if self._closed:
            raise ValueError()
        raise io.UnsupportedOperation()

    def readlines(self, hint: int = -1) -> Union[List[str], List[bytes]]:
        if self.readable() and not self._closed:
            if 'b' in self._sorted_mode:
                if isinstance(self._buffer, memoryview):
                    raise io.UnsupportedOperation()
                assert isinstance(self._buffer, mmap.mmap)
                lines: List[bytes] = []
                while True:
                    line = self._buffer.readline()
                    if len(line) == 0:
                        return lines
                    lines.append(line)
            assert isinstance(self._buffer, io.StringIO)
            return self._buffer.readlines(hint)
        if self._closed:
            raise ValueError()
        raise io.UnsupportedOperation()

    def read(self, size: int = -1) -> Union[str, bytes, memoryview]:
        if size < -1:
            raise ValueError()
        if self.readable() and not self._closed:
            if isinstance(self._buffer, memoryview):
                if size == -1:
                    data = self._buffer[self._pos:]
                    self._pos = len(self._buffer)
                    return data
                data = self._buffer[self._pos:self._pos + size]
                self._pos = min(self._pos + size, len(self._buffer))
                return data
            assert isinstance(self._buffer, (mmap.mmap, io.StringIO))
            return self._buffer.read(size)
        if self._closed:
            raise ValueError()
        raise io.UnsupportedOperation()

    def write(self, data: Any) -> int:
        if self.writable() and not self._closed:
            assert isinstance(self._buffer, (mmap.mmap, io.StringIO))
            if 'a' in self._sorted_mode:
                self.seek(0, 2)
            if isinstance(self._buffer, mmap.mmap):
                written = self._buffer.write(data)
                self._extent = max(self._extent, self._buffer.tell())
            else:
                assert isinstance(data, str)
                written = self._buffer.write(data)
                self._extent = max(self._extent, self._buffer.tell())
            return written
        if self._closed:
            raise ValueError()
        raise io.UnsupportedOperation()

    def writelines(
        self,
        lines: List[Any]
    ):
        for line in lines:
            self.write(line)

    def _save_buffer(self):
        with transaction_context(self._env, write = True):
            if isinstance(self._buffer, mmap.mmap):
                self._size = self._extent
                self._content_binary = memoryview(self._buffer)[0:self._extent]
            else:
                content = self._buffer.getvalue()
                self._size = len(content)
                self._content_binary = content.encode(self._encoding)

    def _load_buffer(self, binary: bool):
        if binary:
            if self._sorted_mode == 'br' or self._sorted_mode == 'brx':
                if bool(thread.local.context.stacks[self._env]):
                    self._pos = 0
                    self._buffer = self._content_binary
                    return
            data = self._content_binary
            self._buffer = mmap.mmap(-1, self._bufsize)
            self._buffer[0:len(data)] = data[:]
            self._extent = len(data)
            if 'a' in self._sorted_mode:
                self.seek(0, 2)
            else:
                self.seek(0, 0)
            return
        self._buffer = io.StringIO(
            codecs.decode(self._content_binary, encoding = self._encoding),
            newline = None
        )
        if 'a' in self._sorted_mode:
            self.seek(0, 2)

    @staticmethod
    def truncate(size = None):
        raise io.UnsupportedOperation()

    @staticmethod
    def flush():
        raise io.UnsupportedOperation()

    @staticmethod
    def fileno() -> int:
        raise IOError()
