"""Compression configuration."""

from pathlib import Path
import os
from enum import Enum


class Compression(Enum):
    """Compression types for files."""

    INFERRED = "INFERRED"
    NONE = "NONE"
    GZIP = "GZIP"

    @classmethod
    def from_file(cls, path: os.PathLike) -> "Compression":
        """Infer the compression type from the file extension."""
        if Path(path).suffix == ".gz":
            return Compression.GZIP
        return Compression.NONE

    def infer_or_use(self, path: os.PathLike) -> "Compression":
        """Infer the compression type from the file extension if needed."""
        if self == Compression.INFERRED:
            return Compression.from_file(path)
        return self
