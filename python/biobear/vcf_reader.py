from pathlib import Path

from .biobear import _VCFReader, _VCFIndexedReader

import polars as pl


class VCFReader:
    """A VCF File Reader.

    This class is used to read a VCF file and convert it to a polars DataFrame.
    """

    def __init__(self, path: Path):
        """Initialize the VCFReader.

        Args:
            path (Path): Path to the VCF file.

        """
        self._vcf_reader = _VCFReader(str(path))

    def read(self) -> pl.DataFrame:
        """Read the VCF file and return a polars DataFrame."""
        return self.to_polars()

    def to_polars(self) -> pl.DataFrame:
        """Read the VCF file and return a polars DataFrame."""
        contents = self._vcf_reader.read()
        return pl.read_ipc(contents)


class VCFIndexedReader:
    """An Indexed VCF File Reader.

    This class is used to read or query an indexed VCF file and convert it to a
    polars DataFrame.

    """

    def __init__(self, path: Path):
        """Initialize the VCFIndexedReader."""
        self._vcf_reader = _VCFIndexedReader(str(path))

    def read(self) -> pl.DataFrame:
        """Read the VCF file and return a polars DataFrame."""
        contents = self._vcf_reader.read()
        return pl.read_ipc(contents)

    def query(self, region: str) -> pl.DataFrame:
        """Query the VCF file and return a polars DataFrame.

        Args:
            region (str): The region to query.

        """
        contents = self._vcf_reader.query(region)
        return pl.read_ipc(contents)
