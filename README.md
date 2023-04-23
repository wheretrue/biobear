# biobear

[![PyPI version](https://badge.fury.io/py/biobear.svg)](https://badge.fury.io/py/biobear)

biobear is a Python library designed for reading and searching bioinformatic file formats, using Rust as its backend. It has minimal dependencies and only requires Polars. Biobear can be used to query indexed VCF and BAM files, as well as read FASTA, FASTQ, GFF, BAM, and VCF files.

## Installation

```bash
pip install biobear
```

## Usage

Read a FASTQ file:

```python
import biobear as bb

df = bb.FastqReader("test.fq").read()
print(df.head())
# ┌─────────┬───────────────────────┬───────────────────────────────────┬───────────────────────────────────┐
# │ name    ┆ description           ┆ sequence                          ┆ quality                           │
# │ ---     ┆ ---                   ┆ ---                               ┆ ---                               │
# │ str     ┆ str                   ┆ str                               ┆ str                               │
# ╞═════════╪═══════════════════════╪═══════════════════════════════════╪═══════════════════════════════════╡
# │ SEQ_ID  ┆ This is a description ┆ GATTTGGGGTTCAAAGCAGTATCGATCAAATA… ┆ !''*((((***+))%%%++)(%%%%).1***-… │
# │ SEQ_ID2 ┆ null                  ┆ GATTTGGGGTTCAAAGCAGTATCGATCAAATA… ┆ !''*((((***+))%%%++)(%%%%).1***-… │
# └─────────┴───────────────────────┴───────────────────────────────────┴───────────────────────────────────┘
```

Query an indexed VCF file.


```python
import biobear as bb

# Will error if test.vcf.gz.tbi is not present
df = bb.VCFIndexedReader("test.vcf.gz").query("1")
print(df.head())
# ┌────────────┬──────────┬───────┬───────────┬───┬───────────────┬────────┬───────────────────────────────────┬────────────────┐
# │ chromosome ┆ position ┆ id    ┆ reference ┆ … ┆ quality_score ┆ filter ┆ info                              ┆ format         │
# │ ---        ┆ ---      ┆ ---   ┆ ---       ┆   ┆ ---           ┆ ---    ┆ ---                               ┆ ---            │
# │ str        ┆ i32      ┆ str   ┆ str       ┆   ┆ f32           ┆ str    ┆ str                               ┆ str            │
# ╞════════════╪══════════╪═══════╪═══════════╪═══╪═══════════════╪════════╪═══════════════════════════════════╪════════════════╡
# │ 1          ┆ 3000150  ┆       ┆ C         ┆ … ┆ 59.200001     ┆ PASS   ┆ AN=4;AC=2                         ┆ GT:GQ          │
# │ 1          ┆ 3000151  ┆       ┆ C         ┆ … ┆ 59.200001     ┆ PASS   ┆ AN=4;AC=2                         ┆ GT:DP:GQ       │
# │ 1          ┆ 3062915  ┆ id3D  ┆ GTTT      ┆ … ┆ 12.9          ┆ q10    ┆ DP4=1,2,3,4;AN=4;AC=2;INDEL;STR=… ┆ GT:GQ:DP:GL    │
# │ 1          ┆ 3062915  ┆ idSNP ┆ G         ┆ … ┆ 12.6          ┆ test   ┆ TEST=5;DP4=1,2,3,4;AN=3;AC=1,1    ┆ GT:TT:GQ:DP:GL │
# │ 1          ┆ 3106154  ┆       ┆ CAAA      ┆ … ┆ 342.0         ┆ PASS   ┆ AN=4;AC=2                         ┆ GT:GQ:DP       │
# └────────────┴──────────┴───────┴───────────┴───┴───────────────┴────────┴───────────────────────────────────┴────────────────┘
```

## Available Readers

There are a slew of readers available, though feel free to open up an Issue or a PR if you'd like one added.

-   `FastaReader`
-   `FastqReader`
-   `VCFReader`
-   `VCFIndexedReader`
-   `BamReader`
-   `BamIndexedReader`
-   `GFFReader`

Generally these all work the same way, in that calling `.read()` on the reader will return a Polars DataFrame. Some do have additional methods, which are documented below.

### `VCFIndexedReader`

This reader takes an indexed BGZF VCF file. It supports `.read()`, but also `.query()`.

```python
import biobear as bb

# Will error if test.vcf.gz.tbi is not present
reader = bb.VCFIndexedReader("test.vcf.gz")
result = reader.query("1:1000-2000")

print(result)
```

### `BamIndexedReader`

This reader takes a BAM file and an index file. It supports `.read()`, but also `.query()`.

```python
import biobear as bb
reader = bb.BamIndexedReader("test.bam", "test.bam.bai")
result = reader.query("chr1", 1, 1000)

print(result)
```

## Limitations

Currently, the library reads the entire file (or query result) into memory. This probably isn't a problem unless you're working with very large sequence files or query results.

## Future Plans

Eventually, I'd like to move over the rest of the code from https://github.com/tshauck/brrrr/ and eventually incorporate better parsing of VCF's (as demonstrated in https://github.com/natir/vcf2parquet and https://www.wheretrue.dev/docs/wtt01/api-reference/table-functions#read_vcf_file_records).

Please open an issue if there's something you'd like to see added!
