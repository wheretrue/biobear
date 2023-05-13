"""BAM File Readers."""

import os

import polars as pl

from biobear.reader import Reader
from .biobear import _BamIndexedReader, _BamReader


class BamReader(Reader):
    """A BAM File Reader."""

    def __init__(self, path: os.PathLike):
        """Initialize the BamReader.

        Args:
            path (Path): Path to the BAM file.

        """
        self._bam_reader = _BamReader(str(path))

    @property
    def inner(self):
        """Return the inner reader."""
        return self._bam_reader


class BamIndexedReader(Reader):
    """An Indexed BAM File Reader."""

    def __init__(self, path: os.PathLike, index: os.PathLike):
        """Initialize the BamIndexedReader.

        Args:
            path (Path): Path to the BAM file.
            index (Path): Path to the BAM index file.

        """
        self._bam_reader = _BamIndexedReader(str(path), str(index))

    @property
    def inner(self):
        """Return the inner reader."""
        return self._bam_reader

    def query(self, chrom: str, start: int, end: int) -> pl.DataFrame:
        """Query the BAM file and return a polars DataFrame.

        Args:
            chrom (str): The chromosome to query.
            start (int): The start position to query.
            end (int): The end position to query.

        """
        contents = self._bam_reader.query(chrom, start, end)
        return pl.read_ipc(contents)
