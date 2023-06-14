# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path

import pytest

from biobear import BamReader, BamIndexedReader

DATA = Path(__file__).parent / "data"


def test_bam_reader():
    reader = BamReader(DATA / "bedcov.bam")
    df = reader.read()

    assert len(df) == 61


def test_bam_reader_no_file():
    with pytest.raises(OSError):
        BamReader("test.bam")


def test_bam_indexed_reader():
    reader = BamIndexedReader(DATA / "bedcov.bam")
    df = reader.query("chr1:12203700-12205426")

    assert len(df) == 1


def test_bam_indexed_reader_no_file():
    with pytest.raises(OSError):
        BamIndexedReader("test.bam")
