from pathlib import Path

from .biobear import _FastaReader, _FastaGzippedReader
from biobear.compression import Compression

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
            compression = compression.from_file(path)

        if compression == Compression.GZIP:
            self._fasta_reader = _FastaGzippedReader(str(path))
        else:
            self._fasta_reader = _FastaReader(str(path))

    def read(self) -> pl.DataFrame:
        return self.to_polars()

    def to_polars(self) -> pl.DataFrame:
        contents = self._fasta_reader.read()
        return pl.read_ipc(contents)
