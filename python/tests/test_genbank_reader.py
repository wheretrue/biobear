# Test the genbank reader can be converted to a polars dataframe

from pathlib import Path

import pytest

from biobear import GenbankReader

DATA = Path(__file__).parent / "data"


def test_genbank_reader():
    reader = GenbankReader(DATA / "BGC0000404.gbk")
    df = reader.read()

    assert len(df) == 1


def test_genbank_missing_file():
    with pytest.raises(OSError):
        GenbankReader(DATA / "missing.gbk")
