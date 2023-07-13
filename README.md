<h1 align="center">
    <img src=".github/biobear.svg" width="400px" alt="biobear" />
</h1>

biobear is a Python library designed for reading and searching bioinformatic file formats, using Rust as its backend and producing Arrow Batch Readers and other downstream formats (like polars or duckdb).

The python package has minimal dependencies and only requires Polars. Biobear can be used to read various bioinformatic file formats, including FASTA, FASTQ, VCF, BAM, and GFF locally or from an object store like S3. It can also query some indexed file formats locally like VCF and BAM.

[![Release](https://github.com/wheretrue/biobear/actions/workflows/release.yml/badge.svg)](https://github.com/wheretrue/biobear/actions/workflows/release.yml)

Please see the [documentation] for information on how to get started using biobear.

[documentation]: https://www.wheretrue.dev/docs/exon/biobear/.

## Quickstart

To install biobear, run:

```bash
pip install biobear
pip install polars # needed for `to_polars` method
```

Create a file with some GFF data:

```bash
echo "chr1\t.\tgene\t1\t100\t.\t+\t.\tgene_id=1;gene_name=foo" > test.gff
echo "chr1\t.\tgene\t200\t300\t.\t+\t.\tgene_id=2;gene_name=bar" >> test.gff
```

Then you can use biobear to read a file:

```python
import biobear as bb

df = bb.GFFReader("test.gff").to_polars()
print(df)
```

This will print:

```text
┌─────────┬────────┬──────┬───────┬───┬───────┬────────┬───────┬───────────────────────────────────┐
│ seqname ┆ source ┆ type ┆ start ┆ … ┆ score ┆ strand ┆ phase ┆ attributes                        │
│ ---     ┆ ---    ┆ ---  ┆ ---   ┆   ┆ ---   ┆ ---    ┆ ---   ┆ ---                               │
│ str     ┆ str    ┆ str  ┆ i64   ┆   ┆ f32   ┆ str    ┆ str   ┆ list[struct[2]]                   │
╞═════════╪════════╪══════╪═══════╪═══╪═══════╪════════╪═══════╪═══════════════════════════════════╡
│ chr1    ┆ .      ┆ gene ┆ 1     ┆ … ┆ null  ┆ +      ┆ null  ┆ [{"gene_id","1"}, {"gene_name","… │
│ chr1    ┆ .      ┆ gene ┆ 200   ┆ … ┆ null  ┆ +      ┆ null  ┆ [{"gene_id","2"}, {"gene_name","… │
└─────────┴────────┴──────┴───────┴───┴───────┴────────┴───────┴───────────────────────────────────┘
```
