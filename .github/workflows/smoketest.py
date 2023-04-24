import biobear

df = biobear.FastaReader("python/tests/data/test.fasta").read()

print(df)
