# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path

from biobear import VCFReader, VCFIndexedReader

DATA = Path(__file__).parent / "data"


def test_vcf_reader():
    reader = VCFReader(DATA / "vcf_file.vcf")
    df = reader.to_polars()

    assert len(df) == 15

def test_sam_indexed_reader_read():
    reader = VCFIndexedReader(DATA / "vcf_file.vcf.gz")
    df = reader.read()

    assert len(df) == 15


def test_sam_indexed_reader_quer():
    reader = VCFIndexedReader(DATA / "vcf_file.vcf.gz")
    df = reader.query("1")

    assert len(df) == 11
