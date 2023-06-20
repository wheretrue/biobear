"""BCF File Readers."""

import os

import polars as pl
import pyarrow.dataset as ds

from biobear.reader import Reader

from .biobear import _ExonReader, _BCFIndexedReader


class BCFReader(Reader):
    """A BCF File Reader.

    This class is used to read a BCF file and convert it to a polars DataFrame.
    """

    def __init__(self, path: os.PathLike):
        """Initialize the BCFReader.

        Args:
            path (Path): Path to the BCF file.

        """
        self._bcf_reader = _ExonReader(str(path), "BCF", None)

    @property
    def inner(self):
        """Return the inner reader."""
        return self._bcf_reader


class BCFIndexedReader(Reader):
    """An Indexed BCF File Reader.

    This class is used to read or query an indexed BCF file and convert it to a
    polars DataFrame.

    """

    def __init__(self, path: os.PathLike):
        """Initialize the BCFIndexedReader."""
        self._bcf_reader = _BCFIndexedReader(str(path))

    @property
    def inner(self):
        """Return the inner reader."""
        return self._bcf_reader

    def query(self, region: str) -> pl.DataFrame:
        """Query the BCF file and return a polars DataFrame.

        Args:
            region (str): The region to query.

        """
        contents = self._bcf_reader.query(region)
        scanner = ds.Scanner.from_batches(contents).to_table()

        return pl.from_arrow(scanner)
