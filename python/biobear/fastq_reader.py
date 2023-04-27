"""FASTQ reader."""
import os

from .biobear import (
    _FastqReader,
    _FastqGzippedReader,
)
from biobear.compression import Compression

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds


class FastqReader:
    def __init__(
        self, path: os.PathLike, compression: Compression = Compression.INFERRED
    ):
        """Read a fastq file.

        Args:
            path (Path): Path to the fastq file.

        Kwargs:
            compression (Compression): Compression type of the file. Defaults to
                Compression.INFERRED.

        """
        self.compression = compression
        if self.compression == Compression.INFERRED:
            self.compression = compression.from_file(path)

        if self.compression == Compression.GZIP:
            self._fastq_reader = _FastqGzippedReader(str(path))
        else:
            self._fastq_reader = _FastqReader(str(path))

    def read(self) -> pl.DataFrame:
        """Read the fasta file and return a polars DataFrame."""
        return pl.from_arrow(self.to_arrow_record_batch_reader().read_all())

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the fasta reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow_record_batch_reader())

    def to_arrow_record_batch_reader(self) -> pa.RecordBatchReader:
        """Convert the fasta reader to an arrow batch reader."""
        return self._fastq_reader.to_pyarrow()
