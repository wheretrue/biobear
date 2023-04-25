# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path

import pytest

from biobear import GFFReader

DATA = Path(__file__).parent / "data"


def test_gff_reader():
    reader = GFFReader(DATA / "test.gff")
    df = reader.read()

    assert len(df) == 2


def test_gff_reader_to_scanner():
    reader = GFFReader(DATA / "test.gff")
    scanner = reader.to_arrow_scanner()

    assert scanner.count_rows() == 2


def test_gff_reader_no_file():
    with pytest.raises(FileNotFoundError):
        GFFReader("test.gff")
