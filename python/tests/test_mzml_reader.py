# Test the fasta reader can be converted to a polars dataframe

import importlib
from pathlib import Path

import pytest

from biobear import MzMLReader

DATA = Path(__file__).parent / "data"


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_mzml_reader_polars():
    reader = MzMLReader(DATA / "test.mzML")
    df = reader.to_polars()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("pandas"), reason="pandas not installed"
)
def test_mzml_reader_pandas():
    reader = MzMLReader(DATA / "test.mzML")
    df = reader.to_pandas()

    assert len(df) == 2


def test_mzml_reader_to_scanner():
    reader = MzMLReader(DATA / "test.mzML")
    scanner = reader.to_arrow_scanner()

    assert scanner.count_rows() == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_mzml_reader_gz():
    reader = MzMLReader(DATA / "test.mzML.gz")
    df = reader.to_polars()

    assert len(df) == 2


def test_mzml_reader_gz_to_scanner():
    reader = MzMLReader(DATA / "test.mzML.gz")
    scanner = reader.to_arrow_scanner()

    assert scanner.count_rows() == 2
