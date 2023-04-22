# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path

from biobear import GFFReader

DATA = Path(__file__).parent / "data"


def test_gff_reader():

    reader = GFFReader(DATA / "test.gff")
    df = reader.to_polars()

    assert len(df) == 2
