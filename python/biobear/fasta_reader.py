"""FASTA file reader."""
from pathlib import Path

from .biobear import (
    _FastaReader,
    _FastaGzippedReader,
)
from biobear.compression import Compression

import pyarrow as pa
import pyarrow.dataset as ds
import polars as pl


class FastaReader:
    def __init__(self, path: Path, compression: Compression = Compression.INFERRED):
        """Read a fasta file.

        Args:
            path (Path): Path to the fasta file.

        Kwargs:
            compression (Compression): Compression type of the file. Defaults to
                Compression.INFERRED.

        """
        self.compression = compression.infer_or_use(path)

        if self.compression == Compression.GZIP:
            self._fasta_reader = _FastaGzippedReader(str(path))
        else:
            self._fasta_reader = _FastaReader(str(path))

    def read(self) -> pl.DataFrame:
        """Read the fasta file and return a polars DataFrame."""
        return pl.from_arrow(self.to_arrow_record_batch_reader().read_all())

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the fasta reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow_record_batch_reader())

    def to_arrow_record_batch_reader(self) -> pa.RecordBatchReader:
        """Convert the fasta reader to an arrow batch reader."""
        return self._fasta_reader.to_pyarrow()
