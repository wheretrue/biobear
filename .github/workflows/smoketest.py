"""Smoke test for biobear."""

import biobear

df = biobear.FastaReader("python/tests/data/test.fasta").to_polars()

assert df.shape == (2, 3)
