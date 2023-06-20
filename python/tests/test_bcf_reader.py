# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path

import pytest

from biobear import BCFReader, BCFIndexedReader

DATA = Path(__file__).parent / "data"


def test_bcf_reader():
    reader = BCFReader(DATA / "index.bcf")
    df = reader.read()

    assert len(df) == 621


def test_bcf_reader_missing_file():
    with pytest.raises(OSError):
        BCFReader("test.bcf")


def test_bcf_indexed_reader_query():
    reader = BCFIndexedReader(DATA / "index.bcf")
    df = reader.query("1")

    assert len(df) == 191

    with pytest.raises(ValueError):
        reader.query("chr1")
