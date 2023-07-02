"""Abstract Reader class for reading data from a file or stream."""

from abc import ABC, abstractmethod

import pyarrow as pa
import pyarrow.dataset as ds


class Reader(ABC):
    """The abstract reader class."""

    @property
    @abstractmethod
    def inner(self):
        """Return the inner reader."""

    def to_polars(self):
        """Read the fasta file and return a polars DataFrame."""
        try:
            import polars as pl
        except ImportError as import_error:
            raise ImportError(
                "The 'polars' package is required to use the to_polars method."
            ) from import_error

        pydict = self.to_arrow_scanner().to_table().to_pydict()
        return pl.from_dict(pydict)

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the fasta reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow())

    def to_arrow(self) -> pa.RecordBatchReader:
        """Convert the fasta reader to an arrow batch reader."""
        return self.inner.to_pyarrow()
