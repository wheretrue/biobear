"""Genbank file reader."""
from pathlib import Path

from .biobear import _GenbankReader

import pyarrow as pa
import pyarrow.dataset as ds
import polars as pl


class GenbankReader:
    def __init__(self, path: Path):
        """Read a fasta file.

        Args:
            path (Path): Path to the fasta file.

        """
        self._reader = _GenbankReader(str(path))

    def read(self) -> pl.DataFrame:
        """Read the fasta file and return a polars DataFrame."""
        # A bit of a hack as polars doesn't like Maps
        pydict = self.to_arrow_scanner().to_table().to_pydict()
        return pl.from_dict(pydict)

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the fasta reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow_record_batch_reader())

    def to_arrow_record_batch_reader(self) -> pa.RecordBatchReader:
        """Convert the fasta reader to an arrow batch reader."""
        return self._reader.to_pyarrow()
