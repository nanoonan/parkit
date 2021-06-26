# pylint: disable = attribute-defined-outside-init, unnecessary-lambda
#
# reviewed:
#
import codecs
import datetime
import logging
import pickle

from typing import (
    Any, Dict, Optional, Union
)

import numpy as np
import pandas as pd

import parkit.storage.threadlocal as thread

from parkit.adapters.fileio import FileIO
from parkit.storage.context import transaction_context
from parkit.utility import (
    create_class,
    get_qualified_class_name
)

logger = logging.getLogger(__name__)

class File(FileIO):

    def __get_pandas_dataframe(self) -> pd.DataFrame:
        try:
            stash = self.mode
            self.mode = 'rb'
            with self:
                return pd.read_feather(self)
        finally:
            self.mode = stash

    @staticmethod
    def __get_numpy_ndarray(
        data: memoryview,
        metadata: Dict[str, Any],
        zero_copy: bool
    ) -> np.ndarray:
        return np.lib.stride_tricks.as_strided(
            np.frombuffer(
                data if zero_copy else bytearray(data),
                create_class('.'.join(['numpy', metadata['content-properties']['dtype']]))
            ),
            metadata['content-properties']['shape'],
            metadata['content-properties']['strides']
        )

    def __get_octet_stream(
        self,
        zero_copy: bool
    ) -> Union[memoryview, bytearray]:
        data = self._content_binary
        if zero_copy:
            return data
        return bytearray(data)

    def get_content(
        self,
        *,
        zero_copy: bool = False
    ) -> Optional[Any]:
        if not self._closed:
            raise ValueError()
        need_copy = not bool(thread.local.context.stacks[self._env])
        with transaction_context(self._env, write = False):
            try:
                metadata = self.metadata
                return {
                    'application/octet-stream': lambda: self.__get_octet_stream(
                        zero_copy if not need_copy else False
                    ),
                    'application/python-pickle': lambda: pickle.loads(self._content_binary),
                    'application/python-pandas-dataframe': lambda: self.__get_pandas_dataframe(),
                    'application/python-numpy-ndarray': lambda: self.__get_numpy_ndarray(
                        self._content_binary,
                        metadata,
                        zero_copy if not need_copy else False
                    ),
                    'text/plain': lambda: codecs.decode(
                        self._content_binary, encoding = self.encoding
                    )
                }[metadata['content-type']]()
            except KeyError:
                return None
        raise ValueError()

    def __set_pandas_dataframe(
        self,
        data: pd.DataFrame,
        metadata: Dict[str, Any]
    ):
        try:
            stash = self.mode
            self.mode = 'wb'
            with self:
                data.to_feather(self)
            metadata['content-type'] = 'application/python-pandas-dataframe'
            metadata['content-properties'] = dict(
                columns = data.columns.to_list(),
                type = get_qualified_class_name(data),
                nrows = len(data)
            )
        finally:
            self.mode = stash

    def __set_numpy_ndarray(
        self,
        data: np.ndarray,
        metadata: Dict[str, Any]
    ):
        self._content_binary = data.data
        self._size = data.data.nbytes
        metadata['content-type'] = 'application/python-numpy-ndarray'
        metadata['content-properties'] = dict(
            shape = data.shape,
            strides = data.strides,
            dtype = str(data.dtype),
            type = get_qualified_class_name(data)
        )

    def set_content(self, value: Any):
        if not self._closed:
            raise ValueError()
        with transaction_context(self._env, write = True):
            metadata = self.metadata
            if isinstance(value, str):
                self._size = len(value)
                self._content_binary = memoryview(value.encode(self.encoding))
                metadata['content-type'] = 'text/plain'
                metadata['content-encoding'] = self.encoding
            elif isinstance(value, (memoryview, bytes, bytearray)):
                self._size = len(value)
                self._content_binary = memoryview(value)
                metadata['content-type'] = 'application/octet-stream'
            elif isinstance(value, pd.DataFrame):
                self.__set_pandas_dataframe(value, metadata)
            elif isinstance(value, np.ndarray):
                self.__set_numpy_ndarray(value, metadata)
            else:
                pickled = pickle.dumps(value)
                self._size = len(pickled)
                self._content_binary = memoryview(pickled)
                metadata['content-type'] = 'application/python-pickle'
                metadata['content-properties'] = dict(
                    type = get_qualified_class_name(value)
                )
            metadata['last-modified'] = str(datetime.datetime.now())
            super(File, self.__class__).metadata.fset(self, metadata) # type: ignore

    @property
    def metadata(self) -> Dict[str, Any]:
        return super().metadata

    @metadata.setter
    def metadata(self, value: Dict[str, Any]):
        with transaction_context(self._env, write = True):
            metadata = self.metadata
            value = {
                k: v
                for k, v in value.items()
                if k not in ['last-modified', 'content-type', 'content-properties']
            }
            metadata.update(value)
            super(File, self.__class__).metadata.fset(self, metadata) # type: ignore
