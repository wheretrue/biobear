from pathlib import Path

from .biobear import _VcfReader, _VcfIndexReader

import polars as pl

class VcfReader:
    def __init__(self, path: Path):
        self._vcf_reader = _VcfReader(str(path))

    def read(self) -> pl.DataFrame:
        return self.to_polars()

    def to_polars(self) -> pl.DataFrame:
        contents = self._vcf_reader.read()
        return pl.read_ipc(contents)

class BamIndexReader:
    def __init__(self, path: Path, index: Path):
        self._vcf_reader = _VcfIndexReader(str(path), str(index))

    def read(self) -> pl.DataFrame:
        return self.to_polars()

    def query(self, region: str) -> pl.DataFrame:
        contents = self._vcf_reader.query(region)
        return pl.read_ipc(contents)

    def to_polars(self) -> pl.DataFrame:
        contents = self._vcf_reader.read()
        return pl.read_ipc(contents)
