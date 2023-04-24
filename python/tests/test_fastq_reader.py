# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path

import pytest

from biobear import FastqReader
from biobear.compression import Compression

DATA = Path(__file__).parent / "data"


def test_fastq_reader():
    fastq_reader = FastqReader(DATA / "test.fastq")
    df = fastq_reader.to_polars()

    assert len(df) == 2

def test_fastq_gzipped_reader():
    # Test that the gzip compression is inferred
    fastq_reader = FastqReader(DATA / "test.fastq.gz")
    df = fastq_reader.to_polars()

    assert len(df) == 2

    # Test that the gzip compression is explicitly set
    fastq_reader = FastqReader(DATA / "test.fastq.gz", Compression.GZIP)
    df = fastq_reader.to_polars()

    assert len(df) == 2


def test_fastq_reader_no_file():
    with pytest.raises(FileNotFoundError):
        FastqReader("test.fastq")
