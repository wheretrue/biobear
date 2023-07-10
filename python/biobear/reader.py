"""Abstract Reader class for reading data from a file or stream."""

from abc import ABC, abstractmethod

import pyarrow as pa
import pyarrow.dataset as ds


class Reader(ABC):
    """An abstract base class (ABC) representing a reader.

    The class defines basic functionalities for conversion, but the specifics must be
    implemented in a subclass.
    """

    @property
    @abstractmethod
    def inner(self):
        """Abstract property for the inner reader.

        Returns:
            The inner reader. The type of the reader is defined by the specific
            subclass.
        """

    def to_polars(self):
        """Convert the inner data to a Polars DataFrame.

        This method first converts the inner reader's data to an Arrow table,
        then to a Python dictionary, and finally to a Polars DataFrame.

        Returns:
            pl.DataFrame: The converted data in a Polars DataFrame.

        Raises:
            ImportError: If the 'polars' package is not installed.
        """
        try:
            import polars as pl
        except ImportError as import_error:
            raise ImportError(
                "The 'polars' package is required to use the to_polars method."
            ) from import_error

        pydict = self.to_arrow_scanner().to_table().to_pydict()
        return pl.from_dict(pydict)

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the inner data to an Arrow scanner.

        This method first converts the inner reader's data to Arrow batches,
        and then forms a scanner from these batches.

        Returns:
            ds.Scanner: The converted data in an Arrow scanner.
        """
        return ds.Scanner.from_batches(self.to_arrow())

    def to_arrow(self) -> pa.RecordBatchReader:
        """Convert the inner data to an Arrow record batch reader.

        If the inner reader is exhausted, this method raises an exception.
        Otherwise, it converts the inner reader's data to an Arrow record batch.

        Returns:
            pa.RecordBatchReader: The converted data in an Arrow record batch reader.

        Raises:
            StopIteration: If the inner reader is exhausted.
        """
        if self.inner.is_exhausted():
            raise StopIteration("The reader is exhausted.")

        return self.inner.to_pyarrow()
