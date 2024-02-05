"""Genbank file reader."""
import os

import pyarrow as pa
import pyarrow.dataset as ds

from biobear.reader import Reader
from biobear.compression import Compression

from .biobear import _ExonReader


class GenbankReader(Reader):
    """Genbank file reader."""

    def __init__(
        self, path: os.PathLike, compression: Compression = Compression.INFERRED
    ):
        """Read a genbank file.

        Args:
            path (Path): Path to the fasta file.
            compression (Compression): Compression type of the file.

        """
        self.compression = compression.infer_or_use(path)

        if self.compression == Compression.GZIP:
            self._reader = _ExonReader(str(path), "GENBANK", "GZIP")
        else:
            self._reader = _ExonReader(str(path), "GENBANK", None)

    def to_polars(self):
        """Read the GFF file and return a polars DataFrame."""
        try:
            import polars as pl
        except ImportError as import_error:
            raise ImportError(
                "The polars library is required to convert a GFF file "
                "to a polars DataFrame."
            ) from import_error

        return pl.from_arrow(self.to_arrow().read_all())

    def to_arrow(self) -> pa.RecordBatchReader:
        """Convert the GFF reader to an arrow batch reader."""
        return self._reader.to_pyarrow()

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the GFF reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow())

    @property
    def inner(self):
        """Return the inner reader."""
        return self._reader
