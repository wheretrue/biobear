"""FASTA file reader."""
from pathlib import Path

from .biobear import (
    _FastaReader,
    _FastaGzippedReader,
    fasta_reader_to_py_arrow,
    fasta_gzipped_reader_to_py_arrow,
)
from biobear.compression import Compression

import pyarrow as pa
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
        if compression == Compression.INFERRED:
            self.compression = compression.from_file(path)

        if self.compression == Compression.GZIP:
            self._fasta_reader = _FastaGzippedReader(str(path))
        else:
            self._fasta_reader = _FastaReader(str(path))

    def read(self) -> pl.DataFrame:
        """Read the fasta file and return a polars DataFrame."""
        return pl.from_arrow(self.to_arrow_record_batch_reader().read_all())

    def to_arrow_record_batch_reader(self) -> pa.RecordBatchReader:
        """Convert the fasta reader to an arrow batch reader."""
        if isinstance(self._fasta_reader, _FastaReader):
            return fasta_reader_to_py_arrow(self._fasta_reader)

        elif isinstance(self._fasta_reader, _FastaGzippedReader):
            return fasta_gzipped_reader_to_py_arrow(self._fasta_reader)

        raise NotImplementedError("Unknown fasta reader type")
