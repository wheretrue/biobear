"""Genbank file reader."""
import os

from biobear.reader import Reader

from .biobear import _GenbankReader


class GenbankReader(Reader):
    """Genbank file reader."""

    def __init__(self, path: os.PathLike):
        """Read a fasta file.

        Args:
            path (Path): Path to the fasta file.

        """
        self._reader = _GenbankReader(str(path))

    @property
    def inner(self):
        """Return the inner reader."""
        return self._reader
