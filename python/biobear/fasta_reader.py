from pathlib import Path

from .biobear import _FastaReader

import polars as pl

class FastaReader:
    def __init__(self, path: Path):
        self._fasta_reader = _FastaReader(str(path))

    def to_polars(self) -> pl.DataFrame:
        contents = self._fasta_reader.read()
        return pl.read_ipc(contents)
