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

"""VCF File Readers."""

import os

import pyarrow as pa

from biobear.reader import Reader

from .biobear import _ExonReader, _VCFIndexedReader


class VCFReader(Reader):
    """A VCF File Reader.

    This class is used to read a VCF file and convert it to a polars DataFrame.
    """

    def __init__(self, path: os.PathLike):
        """Initialize the VCFReader.

        Args:
            path (Path): Path to the VCF file.

        """
        self._vcf_reader = _ExonReader(str(path), "VCF", None)

    @property
    def inner(self):
        """Return the inner reader."""
        return self._vcf_reader


class VCFIndexedReader(Reader):
    """An Indexed VCF File Reader.

    This class is used to read or query an indexed VCF file and convert it to a
    polars DataFrame.

    """

    def __init__(self, path: os.PathLike):
        """Initialize the VCFIndexedReader."""
        self._vcf_reader = _VCFIndexedReader(str(path))

    @property
    def inner(self):
        """Return the inner reader."""
        return self._vcf_reader

    def query(self, region: str) -> pa.RecordBatchReader:
        """Query the VCF file and return a pyarrow RecordBatchReader.

        Args:
            region (str): The region to query.

        """
        return self._vcf_reader.query(region)
