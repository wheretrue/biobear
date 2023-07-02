"""GTF File Reader."""

import os

import pyarrow as pa
import pyarrow.dataset as ds

from biobear.compression import Compression
from biobear.reader import Reader

from .biobear import _ExonReader


class GTFReader(Reader):
    """A GTF File Reader."""

    def __init__(
        self, path: os.PathLike, compression: Compression = Compression.INFERRED
    ):
        """Initialize the GTFReader.

        Args:
            path: The path to the GTF file.
        """

        self.compression = compression.infer_or_use(path)

        if self.compression == Compression.GZIP:
            self._gtf_reader = _ExonReader(str(path), "GTF", "GZIP")
        else:
            self._gtf_reader = _ExonReader(str(path), "GTF", None)

    def to_polars(self):
        """Read the GTF file and return a polars DataFrame."""

        try:
            import polars as pl
        except ImportError as import_error:
            raise ImportError(
                "The polars library is required to convert a GTF file to "
                "a polars DataFrame."
            ) from import_error

        return pl.from_arrow(self.to_arrow().read_all())

    def to_arrow(self) -> pa.RecordBatchReader:
        """Convert the GTF reader to an arrow batch reader."""
        return self._gtf_reader.to_pyarrow()

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the GTF reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow())

    @property
    def inner(self):
        """Return the inner reader."""
        return self._gtf_reader
