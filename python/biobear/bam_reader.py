from pathlib import Path

from .biobear import _BamReader, _BamIndexedReader

import polars as pl


class BamReader:
    """A BAM File Reader."""

    def __init__(self, path: Path):
        """Initialize the BamReader.

        Args:
            path (Path): Path to the BAM file.

        """
        self._bam_reader = _BamReader(str(path))

    def read(self) -> pl.DataFrame:
        """Read the BAM file and return a polars DataFrame."""
        contents = self._bam_reader.read()
        return pl.read_ipc(contents)


class BamIndexedReader:
    """An Indexed BAM File Reader."""

    def __init__(self, path: Path, index: Path):
        """Initialize the BamIndexedReader.

        Args:
            path (Path): Path to the BAM file.
            index (Path): Path to the BAM index file.

        """
        self._bam_reader = _BamIndexedReader(str(path), str(index))

    def read(self) -> pl.DataFrame:
        """Read the BAM file and return a polars DataFrame."""
        contents = self._bam_reader.read()
        return pl.read_ipc(contents)

    def query(self, chrom: str, start: int, end: int) -> pl.DataFrame:
        """Query the BAM file and return a polars DataFrame.

        Args:
            chrom (str): The chromosome to query.
            start (int): The start position to query.
            end (int): The end position to query.

        """
        contents = self._bam_reader.query(chrom, start, end)
        return pl.read_ipc(contents)
