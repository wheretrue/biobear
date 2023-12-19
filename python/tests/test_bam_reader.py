# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path
import importlib

import pytest

from biobear import BamReader, BamIndexedReader

DATA = Path(__file__).parent / "data"


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_bam_reader():
    reader = BamReader(DATA / "bedcov.bam")

    df = reader.to_polars()

    assert len(df) == 61


@pytest.mark.skipif(
    not importlib.util.find_spec("pandas"), reason="pandas not installed"
)
def test_bam_reader_to_pandas():
    reader = BamReader(DATA / "bedcov.bam")

    df = reader.to_pandas()

    assert len(df) == 61


def test_bam_indexed_reader():
    reader = BamIndexedReader(DATA / "bedcov.bam")
    rbr = reader.query("chr1:12203700-12205426")

    assert 1 == sum(b.num_rows for b in rbr)
