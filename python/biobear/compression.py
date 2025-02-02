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

"""Compression configuration."""

import os
from enum import Enum
from pathlib import Path


class Compression(Enum):
    """Compression types for files."""

    INFERRED = "INFERRED"
    NONE = "NONE"
    GZIP = "GZIP"
    BZIP2 = "BZIP2"

    @classmethod
    def from_file(cls, path: os.PathLike) -> "Compression":
        """Infer the compression type from the file extension."""
        if Path(path).suffix == ".gz":
            return Compression.GZIP
        if Path(path).suffix == ".bz2":
            return Compression.BZIP2
        return Compression.NONE

    def infer_or_use(self, path: os.PathLike) -> "Compression":
        """Infer the compression type from the file extension if needed."""
        if self == Compression.INFERRED:
            return Compression.from_file(path)
        return self
