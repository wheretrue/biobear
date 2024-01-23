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

"""FASTQ reader."""
import os

from biobear.reader import Reader
from biobear.compression import Compression

from .biobear import _ExonReader


class FastqReader(Reader):
    """FASTQ file reader."""

    def __init__(
        self, path: os.PathLike, compression: Compression = Compression.INFERRED
    ):
        """Read a fastq file.

        Args:
            path (Path): Path to the fastq file.

        Kwargs:
            compression (Compression): Compression type of the file. Defaults to
                Compression.INFERRED.

        """

        self.compression = compression.infer_or_use(path)

        if self.compression == Compression.GZIP:
            self._fastq_reader = _ExonReader(str(path), "FASTQ", "GZIP")
        else:
            self._fastq_reader = _ExonReader(str(path), "FASTQ", None)

    @property
    def inner(self):
        """Return the inner reader."""
        return self._fastq_reader
