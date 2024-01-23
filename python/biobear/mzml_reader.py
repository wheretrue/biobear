"""MzML File Reader."""

import os

from biobear.compression import Compression
from biobear.reader import Reader

from .biobear import _ExonReader


class MzMLReader(Reader):
    """A MzML File Reader."""

    def __init__(
        self, path: os.PathLike, compression: Compression = Compression.INFERRED
    ):
        """Initialize the MzMLReader.

        Args:
            path: The path to the MzML file.
        """

        self.compression = compression.infer_or_use(path)

        if self.compression == Compression.GZIP:
            self._reader = _ExonReader(str(path), "MZML", "GZIP")
        else:
            self._reader = _ExonReader(str(path), "MZML", None)

    @property
    def inner(self):
        """Return the inner reader."""
        return self._reader
