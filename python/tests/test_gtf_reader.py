# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path

import pytest

import polars as pl
from biobear import GTFReader

DATA = Path(__file__).parent / "data"


def test_gtf_reader():
    reader = GTFReader(DATA / "test.gtf")
    df = reader.read()

    assert len(df) == 77


def test_gtf_attr_struct():
    reader = GTFReader(DATA / "test.gtf")
    df = reader.read()

    dtype = df.select(pl.col("attributes")).dtypes[0]

    key_field = pl.Field("key", pl.Utf8)
    value_field = pl.Field("value", pl.Utf8)
    assert dtype == pl.List(pl.Struct([key_field, value_field]))


def test_gtf_reader_to_scanner():
    reader = GTFReader(DATA / "test.gtf")
    scanner = reader.to_arrow_scanner()

    assert scanner.count_rows() == 77


def test_gtf_reader_no_file():
    with pytest.raises(OSError):
        GTFReader("test.gtf")


def test_gtf_reader_gz():
    reader = GTFReader(DATA / "test.gtf.gz")
    df = reader.read()

    assert len(df) == 77


def test_gtf_reader_gz_to_scanner():
    reader = GTFReader(DATA / "test.gtf.gz")
    scanner = reader.to_arrow_scanner()

    assert scanner.count_rows() == 77


def test_gtf_gz_no_file():
    with pytest.raises(OSError):
        GTFReader("test.gtf.gz")
