"""GFF File Reader."""

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds

from .biobear import _GFFReader, gff_reader_to_pyarrow


class GFFReader:
    """A GFF File Reader."""

    def __init__(self, path: Path):
        """Initialize the GFFReader.

        Args:
            path: The path to the GFF file.
        """

        self._gff_reader = _GFFReader(str(path))

    def read(self) -> pl.DataFrame:
        """Read the GFF file and return a polars DataFrame."""
        return pl.from_arrow(self.to_arrow_record_batch_reader().read_all())

    def to_arrow_record_batch_reader(self) -> pa.RecordBatchReader:
        """Convert the GFF reader to an arrow batch reader."""
        return gff_reader_to_pyarrow(self._gff_reader)

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the GFF reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow_record_batch_reader())
