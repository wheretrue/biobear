from pathlib import Path

from .biobear import _BamReader, _BamIndexReader

import polars as pl

class BamReader:
    def __init__(self, path: Path):
        self._bam_reader = _BamReader(str(path))

    def read(self) -> pl.DataFrame:
        return self.to_polars()

    def to_polars(self) -> pl.DataFrame:
        contents = self._bam_reader.read()
        return pl.read_ipc(contents)

class BamIndexReader:
    def __init__(self, path: Path, index: Path):
        self._bam_reader = _BamIndexReader(str(path), str(index))

    def read(self) -> pl.DataFrame:
        return self.to_polars()

    def query(self, chrom: str, start: int, end: int) -> pl.DataFrame:
        contents = self._bam_reader.query(chrom, start, end)
        return pl.read_ipc(contents)

    def to_polars(self) -> pl.DataFrame:
        contents = self._bam_reader.read()
        return pl.read_ipc(contents)
