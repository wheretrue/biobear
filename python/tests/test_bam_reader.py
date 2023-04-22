# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path

from biobear import BamReader, BamIndexedReader

DATA = Path(__file__).parent / "data"


def test_bam_reader():
    reader = BamReader(DATA / "bedcov.bam")
    df = reader.to_polars()

    print(df)

    assert len(df) == 61

def test_sam_indexed_reader():
    reader = BamIndexedReader(DATA / "bedcov.bam", DATA / "bedcov.bam.bai")
    df = reader.query("chr1", 12203700, 12205426)

    print(df)

    assert len(df) == 1
