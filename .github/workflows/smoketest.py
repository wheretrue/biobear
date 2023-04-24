import biobear

df = biobear.FastaReader("python/tests/data/test.fasta").read()

assert df.shape == (2, 3)
