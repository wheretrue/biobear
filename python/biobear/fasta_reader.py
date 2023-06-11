"""FASTA file reader."""
import os

from biobear.reader import Reader
from biobear.compression import Compression

from .biobear import _ExonReader


class FastaReader(Reader):
    """FASTA file reader."""

    def __init__(
        self, path: os.PathLike, compression: Compression = Compression.INFERRED
    ):
        """Read a fasta file.

        Args:
            path (Path): Path to the fasta file.

        Kwargs:
            compression (Compression): Compression type of the file. Defaults to
                Compression.INFERRED.

        """
        self.compression = compression.infer_or_use(path)

        if self.compression == Compression.GZIP:
            self._fasta_reader = _ExonReader(str(path), "FASTA", "GZIP")
        else:
            self._fasta_reader = _ExonReader(str(path), "FASTA", None)

    @property
    def inner(self):
        """Return the inner reader."""
        return self._fasta_reader
