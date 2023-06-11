"""GFF File Reader."""

import os

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds

from biobear.compression import Compression
from biobear.reader import Reader

from .biobear import _ExonReader


class GFFReader(Reader):
    """A GFF File Reader."""

    def __init__(
        self, path: os.PathLike, compression: Compression = Compression.INFERRED
    ):
        """Initialize the GFFReader.

        Args:
            path: The path to the GFF file.
        """

        self.compression = compression.infer_or_use(path)

        if self.compression == Compression.GZIP:
            self._gff_reader = _ExonReader(str(path), "GFF", "GZIP")
        else:
            self._gff_reader = _ExonReader(str(path), "GFF", None)

    def read(self) -> pl.DataFrame:
        """Read the GFF file and return a polars DataFrame."""
        return pl.from_arrow(self.to_arrow_record_batch_reader().read_all())

    def to_arrow_record_batch_reader(self) -> pa.RecordBatchReader:
        """Convert the GFF reader to an arrow batch reader."""
        return self._gff_reader.to_pyarrow()

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the GFF reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow_record_batch_reader())

    @property
    def inner(self):
        """Return the inner reader."""
        return self._gff_reader
