"""VCF File Readers."""

import os

import polars as pl
import pyarrow.dataset as ds

from biobear.reader import Reader

from .biobear import _ExonReader, _VCFIndexedReader


class VCFReader(Reader):
    """A VCF File Reader.

    This class is used to read a VCF file and convert it to a polars DataFrame.
    """

    def __init__(self, path: os.PathLike):
        """Initialize the VCFReader.

        Args:
            path (Path): Path to the VCF file.

        """
        self._vcf_reader = _ExonReader(str(path), "VCF", None)

    @property
    def inner(self):
        """Return the inner reader."""
        return self._vcf_reader


class VCFIndexedReader(Reader):
    """An Indexed VCF File Reader.

    This class is used to read or query an indexed VCF file and convert it to a
    polars DataFrame.

    """

    def __init__(self, path: os.PathLike):
        """Initialize the VCFIndexedReader."""
        self._vcf_reader = _VCFIndexedReader(str(path))

    @property
    def inner(self):
        """Return the inner reader."""
        return self._vcf_reader

    def query(self, region: str) -> pl.DataFrame:
        """Query the VCF file and return a polars DataFrame.

        Args:
            region (str): The region to query.

        """
        contents = self._vcf_reader.query(region)
        scanner = ds.Scanner.from_batches(contents).to_table()

        return pl.from_arrow(scanner)
