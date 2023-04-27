"""BAM File Readers."""

from pathlib import Path

from .biobear import (
    _BamReader,
    _BamIndexedReader,
)

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds


class BamReader:
    """A BAM File Reader."""

    def __init__(self, path: Path):
        """Initialize the BamReader.

        Args:
            path (Path): Path to the BAM file.

        """
        self._bam_reader = _BamReader(str(path))

    def to_arrow_record_batch_reader(self) -> pa.RecordBatchReader:
        """Convert the BAM reader to an arrow batch reader."""
        return self._bam_reader.to_pyarrow()

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the BAM reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow_record_batch_reader())

    def read(self) -> pl.DataFrame:
        """Read the BAM file and return a polars DataFrame."""
        return pl.from_arrow(self.to_arrow_record_batch_reader().read_all())


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
        return pl.from_arrow(self.to_arrow_record_batch_reader().read_all())

    def to_arrow_record_batch_reader(self) -> pa.RecordBatchReader:
        """Convert the BAM reader to an arrow batch reader."""
        return self._bam_reader.to_pyarrow()

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the BAM reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow_record_batch_reader())

    def query(self, chrom: str, start: int, end: int) -> pl.DataFrame:
        """Query the BAM file and return a polars DataFrame.

        Args:
            chrom (str): The chromosome to query.
            start (int): The start position to query.
            end (int): The end position to query.

        """
        contents = self._bam_reader.query(chrom, start, end)
        return pl.read_ipc(contents)
