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

"""BAM File Readers."""

import os

import pyarrow as pa

from biobear.reader import Reader
from .biobear import _BamIndexedReader, _ExonReader


class BamReader(Reader):
    """A BAM File Reader."""

    def __init__(self, path: os.PathLike):
        """Initialize the BamReader.

        Args:
            path (Path): Path to the BAM file.

        """
        self._bam_reader = _ExonReader(str(path), "BAM", None)

    @property
    def inner(self):
        """Return the inner reader."""
        return self._bam_reader


class BamIndexedReader(Reader):
    """An Indexed BAM File Reader."""

    def __init__(self, path: os.PathLike):
        """Initialize the BamIndexedReader.

        Args:
            path (Path): Path to the BAM file.
            index (Path): Path to the BAM index file.

        """
        self._bam_reader = _BamIndexedReader(str(path))

    @property
    def inner(self):
        """Return the inner reader."""
        return self._bam_reader

    def query(self, region: str) -> pa.RecordBatchReader:
        """Query the BAM file and return an Arrow RecordBatchReader.

        Args:
            region: A region in the format "chr:start-end".

        """
        return self._bam_reader.query(region)
