"""BAM File Readers."""

import os

import polars as pl
import pyarrow.dataset as ds

from biobear.reader import Reader
from .biobear import _BamIndexedReader, _ExonReader


class BamReader(Reader):
    """A BAM File Reader."""

    def __init__(self, path: os.PathLike):
        """Initialize the BamReader.

        Args:
            path (Path): Path to the BAM file.

        """
        self._bam_reader = _ExonReader(str(path), "BAM", None)

    @property
    def inner(self):
        """Return the inner reader."""
        return self._bam_reader


class BamIndexedReader(Reader):
    """An Indexed BAM File Reader."""

    def __init__(self, path: os.PathLike):
        """Initialize the BamIndexedReader.

        Args:
            path (Path): Path to the BAM file.
            index (Path): Path to the BAM index file.

        """
        self._bam_reader = _BamIndexedReader(str(path))

    @property
    def inner(self):
        """Return the inner reader."""
        return self._bam_reader

    def query(self, region: str) -> pl.DataFrame:
        """Query the BAM file and return a polars DataFrame.

        Args:
            region: A region in the format "chr:start-end".

        """
        contents = self._bam_reader.query(region)
        scanner = ds.Scanner.from_batches(contents).to_table()

        return pl.from_arrow(scanner)
