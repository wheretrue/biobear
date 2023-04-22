from pathlib import Path

from .biobear import _GFFReader

import polars as pl

class GFFReader:
    def __init__(self, path: Path):
        self._gff_reader = _GFFReader(str(path))

    def read(self) -> pl.DataFrame:
        return self.to_polars()

    def to_polars(self) -> pl.DataFrame:
        contents = self._gff_reader.read()
        return pl.read_ipc(contents)
