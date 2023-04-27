"""VCF File Readers."""

from pathlib import Path

from .biobear import (
    _VCFReader,
    _VCFIndexedReader,
)

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds


class VCFReader:
    """A VCF File Reader.

    This class is used to read a VCF file and convert it to a polars DataFrame.
    """

    def __init__(self, path: Path):
        """Initialize the VCFReader.

        Args:
            path (Path): Path to the VCF file.

        """
        self._vcf_reader = _VCFReader(str(path))

    def to_arrow_record_batch_reader(self) -> pa.RecordBatchReader:
        """Convert the VCF reader to an arrow batch reader."""
        return self._vcf_reader.to_pyarrow()

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the VCF reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow_record_batch_reader())

    def read(self) -> pl.DataFrame:
        """Read the VCF file and return a polars DataFrame."""
        return pl.from_arrow(self.to_arrow_record_batch_reader().read_all())


class VCFIndexedReader:
    """An Indexed VCF File Reader.

    This class is used to read or query an indexed VCF file and convert it to a
    polars DataFrame.

    """

    def __init__(self, path: Path):
        """Initialize the VCFIndexedReader."""
        self._vcf_reader = _VCFIndexedReader(str(path))

    def read(self) -> pl.DataFrame:
        """Read the VCF file and return a polars DataFrame."""
        return pl.from_arrow(self.to_arrow_record_batch_reader().read_all())

    def to_arrow_record_batch_reader(self) -> pa.RecordBatchReader:
        """Convert the VCF reader to an arrow batch reader."""
        return self._vcf_reader.to_pyarrow()

    def to_arrow_scanner(self) -> ds.Scanner:
        """Convert the VCF reader to an arrow scanner."""
        return ds.Scanner.from_batches(self.to_arrow_record_batch_reader())

    def query(self, region: str) -> pl.DataFrame:
        """Query the VCF file and return a polars DataFrame.

        Args:
            region (str): The region to query.

        """
        contents = self._vcf_reader.query(region)
        return pl.read_ipc(contents)
