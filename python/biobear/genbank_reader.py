"""Genbank file reader."""
import os

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

    @property
    def inner(self):
        """Return the inner reader."""
        return self._reader
