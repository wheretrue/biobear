# Test the fasta reader can be converted to a polars dataframe

import importlib
from pathlib import Path

import pytest

from biobear import GFFReader

DATA = Path(__file__).parent / "data"


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_gff_reader_polars():
    reader = GFFReader(DATA / "test.gff")
    df = reader.to_polars()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("pandas"), reason="pandas not installed"
)
def test_gff_reader_pandas():
    reader = GFFReader(DATA / "test.gff")
    df = reader.to_pandas()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_gff_attr_struct():
    import polars as pl

    reader = GFFReader(DATA / "test.gff")
    df = reader.to_polars()
    dtype = df.select(pl.col("attributes")).dtypes[0]

    key_field = pl.Field("key", pl.Utf8)
    value_field = pl.Field("value", pl.List(pl.Utf8))
    assert dtype == pl.List(pl.Struct([key_field, value_field]))


def test_gff_reader_to_scanner():
    reader = GFFReader(DATA / "test.gff")
    scanner = reader.to_arrow_scanner()

    assert scanner.count_rows() == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_gff_reader_gz():
    reader = GFFReader(DATA / "test.gff.gz")
    df = reader.to_polars()

    assert len(df) == 2


def test_gff_reader_gz_to_scanner():
    reader = GFFReader(DATA / "test.gff.gz")
    scanner = reader.to_arrow_scanner()

    assert scanner.count_rows() == 2
