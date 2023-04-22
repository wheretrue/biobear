# Test the fasta reader can be converted to a polars dataframe

from pathlib import Path

import polars as pl
from biobear import FastqReader

DATA = Path(__file__).parent / "data"


def test_fastq_reader():

    fastq_reader = FastqReader(DATA / "test.fastq")
    df = fastq_reader.to_polars()

    assert len(df) == 2
