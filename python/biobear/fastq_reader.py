from pathlib import Path

from .biobear import _FastqReader

import polars as pl

class FastqReader:
    def __init__(self, path: Path):
        self._fastq_reader = _FastqReader(str(path))

    def read(self) -> pl.DataFrame:
        return self.to_polars()

    def to_polars(self) -> pl.DataFrame:
        contents = self._fastq_reader.read()
        return pl.read_ipc(contents)
