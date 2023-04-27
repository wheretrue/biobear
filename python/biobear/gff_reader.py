"""GFF File Reader."""

from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds

from .biobear import _GFFReader, _GFFGzippedReader
from biobear.compression import Compression


class GFFReader:
    """A GFF File Reader."""

    def __init__(self, path: Path, compression: Compression = Compression.INFERRED):
        """Initialize the GFFReader.

        Args:
            path: The path to the GFF file.
        """

        self.compression = compression.infer_or_use(path)

        if self.compression == Compression.GZIP:
            self._gff_reader = _GFFGzippedReader(str(path))
        else:
            self._gff_reader = _GFFReader(str(path))

    def read(self) -> pl.DataFrame:
        """Read the GFF file and return a polars DataFrame."""
        return pl.from_arrow(self.to_arrow_record_batch_reader().read_all())

    def to_arrow_record_batch_reader(self) -> pa.RecordBatchReader:
        """Convert the GFF reader to an arrow batch reader."""
        return self._gff_reader.to_pyarrow()

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the GFF reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow_record_batch_reader())
