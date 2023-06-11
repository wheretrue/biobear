"""FASTQ reader."""
import os

from biobear.reader import Reader
from biobear.compression import Compression

from .biobear import _ExonReader


class FastqReader(Reader):
    """FASTQ file reader."""

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

        self.compression = compression.infer_or_use(path)

        if self.compression == Compression.GZIP:
            self._fastq_reader = _ExonReader(str(path), "FASTQ", "GZIP")
        else:
            self._fastq_reader = _ExonReader(str(path), "FASTQ", None)

    @property
    def inner(self):
        """Return the inner reader."""
        return self._fastq_reader
