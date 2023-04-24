from pathlib import Path
import os
from enum import Enum


class Compression(Enum):
    """Compression types for files."""

    INFERRED = "INFERRED"
    NONE = "NONE"
    GZIP = "GZIP"

    def __str__(self):
        return self.name.lower()

    def __repr__(self):
        return self.name.lower()

    def from_file(self, path: os.PathLike) -> "Compression":
        """Infer the compression type from the file extension."""
        if Path(path).suffix == ".gz":
            return Compression.GZIP
        return Compression.NONE
