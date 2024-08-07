<h1 align="center">
    <img src="https://raw.githubusercontent.com/wheretrue/biobear/main/.github/biobear.svg" width="450px" alt="biobear" />
</h1>

biobear is a Python library designed for reading and searching bioinformatic file formats, using Rust as its backend and producing Arrow Batch Readers and other downstream formats (like polars or duckdb).

The python package has minimal dependencies and only requires Polars. Biobear can be used to read various bioinformatic file formats, including FASTA, FASTQ, VCF, BAM, and GFF locally or from an object store like S3. It can also query some indexed file formats locally like VCF and BAM.

[![Release](https://github.com/wheretrue/biobear/actions/workflows/release.yml/badge.svg)](https://github.com/wheretrue/biobear/actions/workflows/release.yml)

Please see the [documentation] for information on how to get started using biobear.

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

session = bb.connect()
df = session.sql("""
    SELECT * FROM gff_scan('test.gff')
""").to_polars()

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

### Using a Session w/ Exon

BioBear exposes a session object that can be used with [exon][] to work with files directly in SQL, then eventually convert them to a DataFrame if needed.

See the [BioBear Docs][documentation] for more information, but in short, you can use the session like this:

```python
import biobear as bb

session = bb.connect()

session.sql("""
CREATE EXTERNAL TABLE gene_annotations_s3 STORED AS GFF LOCATION 's3://BUCKET/TenflaDSM28944/IMG_Data/Ga0451106_prodigal.gff'
""")

df = session.sql("""
    SELECT * FROM gene_annotations_s3 WHERE score > 50
""").to_polars()
df.head()
# shape: (5, 9)
# ┌──────────────┬─────────────────┬──────┬───────┬───┬────────────┬────────┬───────┬───────────────────────────────────┐
# │ seqname      ┆ source          ┆ type ┆ start ┆ … ┆ score      ┆ strand ┆ phase ┆ attributes                        │
# │ ---          ┆ ---             ┆ ---  ┆ ---   ┆   ┆ ---        ┆ ---    ┆ ---   ┆ ---                               │
# │ str          ┆ str             ┆ str  ┆ i64   ┆   ┆ f32        ┆ str    ┆ str   ┆ list[struct[2]]                   │
# ╞══════════════╪═════════════════╪══════╪═══════╪═══╪════════════╪════════╪═══════╪═══════════════════════════════════╡
# │ Ga0451106_01 ┆ Prodigal v2.6.3 ┆ CDS  ┆ 2     ┆ … ┆ 54.5       ┆ -      ┆ 0     ┆ [{"ID",["Ga0451106_01_2_238"]}, … │
# │ Ga0451106_01 ┆ Prodigal v2.6.3 ┆ CDS  ┆ 228   ┆ … ┆ 114.0      ┆ -      ┆ 0     ┆ [{"ID",["Ga0451106_01_228_941"]}… │
# │ Ga0451106_01 ┆ Prodigal v2.6.3 ┆ CDS  ┆ 1097  ┆ … ┆ 224.399994 ┆ +      ┆ 0     ┆ [{"ID",["Ga0451106_01_1097_2257"… │
# │ Ga0451106_01 ┆ Prodigal v2.6.3 ┆ CDS  ┆ 2261  ┆ … ┆ 237.699997 ┆ +      ┆ 0     ┆ [{"ID",["Ga0451106_01_2261_3787"… │
# │ Ga0451106_01 ┆ Prodigal v2.6.3 ┆ CDS  ┆ 3784  ┆ … ┆ 114.400002 ┆ +      ┆ 0     ┆ [{"ID",["Ga0451106_01_3784_4548"… │
# └──────────────┴─────────────────┴──────┴───────┴───┴────────────┴────────┴───────┴───────────────────────────────────┘
```

## Ecosystem

BioBear aims to make it simple to move easily to and from different prominent data tools in Python. Generally, if the tool can read Arrow or Polars, it can read BioBear's output. To see examples of how to use BioBear with other tools, see:

* [DuckDB][DuckDB Integration]
* [GenomicRanges]
* [DeltaLake]


## Performance

Please see the [exon][]'s performance metrics for thorough benchmarks, but in short, biobear is generally faster than other Python libraries for reading bioinformatic file formats.

For example, here's quick benchmarks for reading one FASTA file with 1 million records and reading 5 FASTA files each with 1 million records for the local file system on an M1 MacBook Pro:

| Library   | 1 file (s)         | 5 files (s)         |
|-----------|--------------------|---------------------|
| BioBear   | 4.605 s ±  0.166 s | 6.420 s ±  0.113 s  |
| BioPython | 6.654 s ±  0.003 s | 34.254 s ±  0.053 s |

The larger difference multiple files is due to biobear's ability to read multiple files in parallel.

[exon]: https://github.com/wheretrue/exon/tree/main/exon-benchmarks
[duckdb]: https://duckdb.org/
[documentation]: https://www.wheretrue.dev/docs/exon/biobear/.
[DuckDB Integration]: https://www.wheretrue.dev/docs/exon/biobear/duckdb-integration
[DeltaLake]: https://www.wheretrue.dev/docs/exon/biobear/delta-lake-integration/
[GenomicRanges]: https://www.wheretrue.dev/docs/exon/biobear/genomicranges-integration
