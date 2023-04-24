import os

from .biobear import _FastqReader, _FastqGzippedReader
from biobear.compression import Compression

import polars as pl

class FastqReader:
    def __init__(
        self,
        path: os.PathLike,
        compression: Compression = Compression.INFERRED
    ):
        """Read a fastq file.

        Args:
            path (Path): Path to the fastq file.

        Kwargs:
            compression (Compression): Compression type of the file. Defaults to
                Compression.INFERRED.

        """
        if compression == Compression.INFERRED:
            compression = compression.from_file(path)

        if compression == Compression.GZIP:
            self._fastq_reader = _FastqGzippedReader(str(path))
        else:
            self._fastq_reader = _FastqReader(str(path))

    def read(self) -> pl.DataFrame:
        return self.to_polars()

    def to_polars(self) -> pl.DataFrame:
        contents = self._fastq_reader.read()
        return pl.read_ipc(contents)
