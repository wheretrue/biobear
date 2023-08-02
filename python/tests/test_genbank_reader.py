"""Test the genbank reader can be converted to a polars dataframe"""

from pathlib import Path
import importlib

import pytest

from biobear import GenbankReader

DATA = Path(__file__).parent / "data"


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_genbank_reader():
    reader = GenbankReader(DATA / "BGC0000404.gbk")
    df = reader.to_polars()

    assert len(df) == 1


@pytest.mark.skipif(
    not importlib.util.find_spec("pandas"), reason="pandas not installed"
)
def test_genbank_reader_to_pandas():
    reader = GenbankReader(DATA / "BGC0000404.gbk")
    df = reader.to_pandas()

    assert len(df) == 1


def test_genbank_missing_file():
    with pytest.raises(OSError):
        GenbankReader(DATA / "missing.gbk")
