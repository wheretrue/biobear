# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path
import importlib

import pytest

from biobear import FastqReader
from biobear import Compression

DATA = Path(__file__).parent / "data"


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_fastq_reader():
    fastq_reader = FastqReader(DATA / "test.fastq")
    df = fastq_reader.to_polars()

    assert len(df) == 2


# Add test for to_pandas() method
@pytest.mark.skipif(
    not importlib.util.find_spec("pandas"), reason="pandas not installed"
)
def test_fastq_reader_to_pandas():
    fastq_reader = FastqReader(DATA / "test.fastq")
    df = fastq_reader.to_pandas()

    assert len(df) == 2

@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_fastq_bgzip_reader():
    fastq_reader = FastqReader(DATA / "fake_fastq_file.fastq.gz", compression=Compression.GZIP)
    df = fastq_reader.to_polars()

    assert len(df) == 20_000

@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_fastq_gzipped_reader():
    # Test that the gzip compression is inferred
    fastq_reader = FastqReader(DATA / "test.fastq.gz")
    df = fastq_reader.to_polars()

    assert len(df) == 2

    # Test that the gzip compression is explicitly set
    fastq_reader = FastqReader(DATA / "test.fastq.gz", Compression.GZIP)
    df = fastq_reader.to_polars()

    assert len(df) == 2


def test_to_arrow_scanner():
    fastq_reader = FastqReader(DATA / "test.fastq")
    scanner = fastq_reader.to_arrow_scanner()

    assert scanner.count_rows() == 2

    gzipped_fastq_reader = FastqReader(DATA / "test.fastq.gz")
    scanner = gzipped_fastq_reader.to_arrow_scanner()

    assert scanner.count_rows() == 2
