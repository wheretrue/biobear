"""Abstract Reader class for reading data from a file or stream."""

from abc import ABC, abstractmethod

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds


class Reader(ABC):
    """The abstract reader class."""

    @property
    @abstractmethod
    def inner(self):
        """Return the inner reader."""

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
        return self.inner.to_pyarrow()
