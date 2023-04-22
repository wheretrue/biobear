from pathlib import Path

from .biobear import _VCFReader, _VCFIndexedReader

import polars as pl

class VCFReader:
    def __init__(self, path: Path):
        self._vcf_reader = _VCFReader(str(path))

    def read(self) -> pl.DataFrame:
        return self.to_polars()

    def to_polars(self) -> pl.DataFrame:
        contents = self._vcf_reader.read()
        return pl.read_ipc(contents)

class VCFIndexedReader:
    def __init__(self, path: Path):
        self._vcf_reader = _VCFIndexedReader(str(path))

    def read(self) -> pl.DataFrame:
        return self.to_polars()

    def query(self, region: str) -> pl.DataFrame:
        contents = self._vcf_reader.query(region)
        return pl.read_ipc(contents)

    def to_polars(self) -> pl.DataFrame:
        contents = self._vcf_reader.read()
        return pl.read_ipc(contents)
