# pylint: disable = broad-except, no-self-use, attribute-defined-outside-init, too-many-public-methods
import io
import logging
import struct

from typing import (
    Any, ByteString, Callable, Dict, Iterable, Iterator, List, Optional, Union
)

import cardinality

import parkit.storage.threadlocal as thread

from parkit.adapters.object import Object
from parkit.storage.context import transaction_context

logger = logging.getLogger(__name__)

valid_modes = [
    'r', 'w', 'a', '+r', '+w', '+a',
    'br', 'bw', 'ab', '+br', '+bw', '+ab',
    'rt', 'tw', 'at', '+rt', '+tw', '+at'
]

class File(Object):

    __pos: int = 0
    __buffer: Optional[Union[bytearray, memoryview, io.StringIO]] = None
    __closed: bool = True
    __sorted_mode: str = 'r'

    def __init__(
        self,
        path: Optional[str] = None,
        /, *,
        mode: Optional[str] = None,
        create: bool = True,
        bind: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
        site: Optional[str] = None,
        on_init: Optional[Callable[[bool], None]] = None
    ):
        def _on_init(create: bool):
            if create:
                self.__size = 0
            if on_init:
                on_init(create)

        super().__init__(
            path, db_properties = [{'integerkey': True}],
            create = create, bind = bind,
            metadata = metadata, site = site,
            on_init = _on_init
        )

        if mode:
            mode = ''.join(sorted(mode))
            if mode not in valid_modes:
                raise ValueError()
            self.__sorted_mode = mode

    def __iter__(self) -> Iterator[Union[str, ByteString]]:
        raise io.UnsupportedOperation()

    @property
    def mode(self) -> str:
        return self.__sorted_mode

    @mode.setter
    def mode(self, value: str):
        value = ''.join(sorted(value))
        if value not in valid_modes:
            raise ValueError()
        if not self.__closed:
            raise ValueError()
        self.__sorted_mode = value

    @property
    def empty(self) -> bool:
        try:
            txn, _, _, implicit = \
            thread.local.context.get(self._Entity__env, write = False)
            result = txn.stat(self._Entity__userdb[0])['entries']
            if implicit:
                txn.commit()
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        return result == 0

    @property
    def size(self) -> int:
        return self.__size

    @property
    def closed(self) -> bool:
        return self.__closed

    def __enter__(self):
        write = not ('r' in self.__sorted_mode and '+' not in self.__sorted_mode)
        thread.local.context.push(self._Entity__env, True, write, True)
        self.__closed = False
        if 'w' not in self.__sorted_mode and not self.empty:
            self.__buffer = self._load__buffer(binary = 'b' in self.__sorted_mode)
        else:
            self.__buffer = bytearray() if 'b' in self.__sorted_mode else io.StringIO()
        if isinstance(self.__buffer, (bytearray, memoryview)):
            self.__pos = 0 if 'a' not in self.__sorted_mode else len(self.__buffer)
        return self

    def __exit__(self, error_type: type, error: Optional[Any], traceback: Any):
        try:
            if self.writable():
                assert isinstance(self.__buffer, (bytearray, io.StringIO))
                self._save__buffer(self.__buffer)
        finally:
            self.__pos = 0
            self.__buffer = None
            self.__closed = True
            thread.local.context.pop(self._Entity__env, error)

    @property
    def content(self) -> Optional[Union[bytearray, bytes, memoryview, str]]:
        if not self.readable() or not self.__closed:
            raise ValueError()
        need_copy = thread.local.context.depth(self._Entity__env) == 0
        with transaction_context(self._Entity__env, write = False):
            if self.empty:
                return None
            data = self._load__buffer(binary = 'b' in self.__sorted_mode)
            if isinstance(data, memoryview):
                if need_copy:
                    return bytes(data)
            elif isinstance(data, io.StringIO):
                return data.getvalue()
            return data

    def encoding(self) -> str:
        return 'utf-8'

    def isatty(self) -> bool:
        return False

    def readable(self) -> bool:
        if '+' in self.__sorted_mode or 'r' in self.__sorted_mode:
            return True
        return False

    def writable(self) -> bool:
        if 'a' in self.__sorted_mode or 'w' in self.__sorted_mode:
            return True
        return False

    def seekable(self) -> bool:
        return True

    def close(self):
        raise io.UnsupportedOperation()

    def tell(self) -> int:
        if not self.__closed:
            if isinstance(self.__buffer, (bytearray, memoryview)):
                assert self.__pos >= 0
                return self.__pos
            assert self.__buffer is not None
            return self.__buffer.tell()
        raise ValueError()

    def seek(self, offset: int, whence: int = 0):
        if not self.__closed:
            if isinstance(self.__buffer, (bytearray, memoryview)):
                pos = self.__pos
                if whence == 0:
                    pos = offset
                elif whence == 1:
                    pos += offset
                elif whence == 2:
                    pos = self.__size + offset
                if pos >= 0:
                    self.__pos = pos
            else:
                assert self.__buffer is not None
                self.__buffer.seek(offset, whence)
            return
        raise ValueError()

    def readline(self, size: int = -1) -> Union[str, bytearray, memoryview]:
        raise io.UnsupportedOperation()

    def readlines(self, hint: int = -1) -> List[Union[str, bytearray, memoryview]]:
        raise io.UnsupportedOperation()

    def read(self, size: int = -1) -> Union[str, bytearray, memoryview]:
        if size < -1:
            raise ValueError()
        if self.readable() and not self.__closed:
            try:
                if 'b' in self.__sorted_mode:
                    assert isinstance(self.__buffer, (bytearray, memoryview))
                    if size == -1:
                        data = self.__buffer[self.__pos:]
                        self.__pos = len(self.__buffer)
                        return data
                    data = self.__buffer[self.__pos:self.__pos + size]
                    self.__pos = min(self.__pos + size, len(self.__buffer))
                    return data
                assert isinstance(self.__buffer, io.StringIO)
                return self.__buffer.read(size)
            except Exception as exc:
                raise IOError() from exc
        if self.__closed:
            raise ValueError()
        raise io.UnsupportedOperation()

    def write(self, data: Union[str, Union[Iterable[int], bytearray, memoryview, bytes]]) -> int:
        if self.writable() and not self.__closed:
            if not isinstance(data, str):
                assert isinstance(self.__buffer, bytearray)
                if 'a' in self.__sorted_mode:
                    self.__pos = len(self.__buffer)
                self.__buffer[self.__pos:] = data
                size = cardinality.count(data)
                self.__pos += size
                return size
            assert isinstance(self.__buffer, io.StringIO)
            return self.__buffer.write(data)
        if self.__closed:
            raise ValueError()
        raise io.UnsupportedOperation()

    def writelines(
        self,
        lines: List[Union[str, Union[Iterable[int], bytearray, memoryview, bytes]]]
    ):
        for line in lines:
            self.write(line)

    def _save__buffer(self, buffer: Union[bytearray, io.StringIO]):
        try:
            txn, cursors, changed, implicit = \
            thread.local.context.get(self._Entity__env, write = True)
            assert not implicit
            cursor = cursors[self._Entity__userdb[0]]
            key = struct.pack('@N', 0)
            if isinstance(buffer, bytearray):
                assert cursor.put(key = key, value = buffer, append = False)
                self.__size = len(buffer)
            else:
                data = buffer.getvalue().encode('utf-8')
                assert cursor.put(key = key, value = data, append = False)
                self.__size = len(data)
            changed.add(self)
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)

    def _load__buffer(self, binary: bool) -> Union[bytearray, memoryview, io.StringIO]:
        try:
            txn, cursors, _, implicit = \
            thread.local.context.get(self._Entity__env, write = False)
            assert not implicit
            cursor = cursors[self._Entity__userdb[0]]
            key = struct.pack('@N', 0)
            if binary:
                if self.__sorted_mode == 'br':
                    return cursor.get(key = key)
                return bytearray(cursor.get(key = key))
            data = cursor.get(key = key)
            data = bytes(data) if isinstance(data, memoryview) else data
            return io.StringIO(data.decode('utf-8'))
        except BaseException as exc:
            self._Entity__abort(exc, txn, implicit)
        raise IOError()

    def truncate(self, size = None):
        raise io.UnsupportedOperation()

    def flush(self):
        raise io.UnsupportedOperation()

    def fileno(self) -> int:
        raise IOError()
