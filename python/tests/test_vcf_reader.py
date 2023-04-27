# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path

import pytest

from biobear import VCFReader, VCFIndexedReader

DATA = Path(__file__).parent / "data"


def test_vcf_reader():
    reader = VCFReader(DATA / "vcf_file.vcf")
    df = reader.read()

    assert len(df) == 15


def test_vcf_reader_missing_file():
    with pytest.raises(OSError):
        VCFReader("test.vcf")


def test_vcf_indexed_reader_read():
    reader = VCFIndexedReader(DATA / "vcf_file.vcf.gz")
    df = reader.read()

    assert len(df) == 15


def test_vcf_indexed_reader_query():
    reader = VCFIndexedReader(DATA / "vcf_file.vcf.gz")
    df = reader.query("1")

    assert len(df) == 11

    with pytest.raises(ValueError):
        reader.query("chr1")


def test_vcf_indexed_reader_query_missing_file():
    with pytest.raises(ValueError):
        VCFIndexedReader("test.vcf.gz")
