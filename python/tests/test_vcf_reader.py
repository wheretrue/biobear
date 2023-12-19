# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path
import importlib

import pytest

from biobear import VCFReader, VCFIndexedReader

DATA = Path(__file__).parent / "data"


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_vcf_reader():
    reader = VCFReader(DATA / "vcf_file.vcf")
    df = reader.to_polars()

    assert len(df) == 15


@pytest.mark.skipif(
    not importlib.util.find_spec("pandas"), reason="pandas not installed"
)
def test_vcf_reader_to_pandas():
    reader = VCFReader(DATA / "vcf_file.vcf")
    df = reader.to_pandas()

    assert len(df) == 15


def test_vcf_indexed_reader_query():
    reader = VCFIndexedReader(DATA / "vcf_file.vcf.gz")
    rbr = reader.query("1")

    assert 11 == sum(b.num_rows for b in rbr)


def test_vcf_indexed_reader_query_no_results():
    reader = VCFIndexedReader(DATA / "vcf_file.vcf.gz")
    rbr = reader.query("chr1")

    assert 0 == sum(b.num_rows for b in rbr)


def test_vcf_indexed_reader_query_missing_file():
    with pytest.raises(ValueError):
        VCFIndexedReader("test.vcf.gz")
