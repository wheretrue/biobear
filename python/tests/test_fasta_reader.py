# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path

import polars as pl
from biobear import FastaReader


def test_fasta_reader(tmp_path: Path):

    tmp_path = tmp_path / "test.fasta"

    with open(tmp_path, "w") as f:
        f.write(">test 1\nACGT\n>test 2\nACGT\n")

    fasta_reader = FastaReader(tmp_path)

    df = fasta_reader.to_polars()

    assert len(df) == 2
