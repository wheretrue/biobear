from pathlib import Path

import polars as pl

from .biobear import _GFFReader


class GFFReader:
    """A GFF File Reader."""

    def __init__(self, path: Path):
        """Initialize the GFFReader."""
        self._gff_reader = _GFFReader(str(path))

    def read(self) -> pl.DataFrame:
        """Read the GFF file and return a polars DataFrame."""
        contents = self._gff_reader.read()
        return pl.read_ipc(contents)
