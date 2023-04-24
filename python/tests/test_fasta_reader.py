# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path
import pytest

from biobear import FastaReader
from biobear.compression import Compression

DATA = Path(__file__).parent / "data"


def test_fasta_reader():
    fasta_reader = FastaReader(DATA / "test.fasta")
    df = fasta_reader.to_polars()

    assert len(df) == 2


def test_fasta_gzipped_reader():
    # Test that the gzip compression is inferred
    fasta_reader = FastaReader(DATA / "test.fasta.gz")
    df = fasta_reader.to_polars()

    assert len(df) == 2

    # Test that the gzip compression is explicitly set
    fasta_reader = FastaReader(DATA / "test.fasta.gz", Compression.GZIP)
    df = fasta_reader.to_polars()

    assert len(df) == 2


def test_fasta_reader_no_file():
    with pytest.raises(FileNotFoundError):
        FastaReader("test.fasta")
