# Copyright 2023 WHERE TRUE Technologies.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""BCF File Readers."""

import os

import pyarrow as pa


from biobear.reader import Reader

from .biobear import _ExonReader, _BCFIndexedReader


class BCFReader(Reader):
    """A BCF File Reader.

    This class is used to read a BCF file and convert it to a polars DataFrame.
    """

    def __init__(self, path: os.PathLike):
        """Initialize the BCFReader.

        Args:
            path (Path): Path to the BCF file.

        """
        self._bcf_reader = _ExonReader(str(path), "BCF", None)

    @property
    def inner(self):
        """Return the inner reader."""
        return self._bcf_reader


class BCFIndexedReader(Reader):
    """An Indexed BCF File Reader.

    This class is used to read or query an indexed BCF file and convert it to a
    polars DataFrame.

    """

    def __init__(self, path: os.PathLike):
        """Initialize the BCFIndexedReader."""
        self._bcf_reader = _BCFIndexedReader(str(path))

    @property
    def inner(self):
        """Return the inner reader."""
        return self._bcf_reader

    def query(self, region: str) -> pa.RecordBatchReader:
        """Query the BCF file and return an arrow batch reader.

        Args:
            region (str): The region to query.

        """
        return self._bcf_reader.query(region)
