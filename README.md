# biobear (v0.3.0)

biobear is a Python library designed for reading and searching bioinformatic file formats, using Rust as its backend and producing Arrow or Polars DataFrames as its output.

The python package has minimal dependencies and only requires Polars. Biobear can be used to read various bioinformatic file formats, including FASTA, FASTQ, VCF, BAM, and GFF. It can also query some indexed file formats, including VCF and BAM.

- [Installation](#installation)
- [Usage](#usage)
- [Similar Packages](#similar-packages)
- [API Documentation](#api-documentation)
  - [vcf\_reader](#vcf_reader)
    - [VCFReader](#vcfreader)
    - [VCFIndexedReader](#vcfindexedreader)
  - [fasta\_reader](#fasta_reader)
    - [FastaReader](#fastareader)
  - [compression](#compression)
    - [Compression](#compression-1)
  - [\_\_init\_\_](#__init__-3)
  - [bam\_reader](#bam_reader)
    - [BamReader](#bamreader)
    - [BamIndexedReader](#bamindexedreader)
  - [fastq\_reader](#fastq_reader)
    - [FastqReader](#fastqreader)
  - [gff\_reader](#gff_reader)
    - [GFFReader](#gffreader)

## Installation

```bash
pip install biobear
```

Prefer python 3.10 or higher, though python 3.7+ should work.

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

Read a gzipped FASTQ file:

```python
import biobear as bb
from biobear.compression import Compression

df = bb.FastqReader("./python/tests/data/test.fastq.gz", compression=Compression.GZIP).read()
print(df.head())
# ┌─────────┬─────────────┬───────────────────────────────────┬───────────────────────────────────┐
# │ name    ┆ description ┆ sequence                          ┆ quality                           │
# │ ---     ┆ ---         ┆ ---                               ┆ ---                               │
# │ str     ┆ str         ┆ str                               ┆ str                               │
# ╞═════════╪═════════════╪═══════════════════════════════════╪═══════════════════════════════════╡
# │ SEQ_ID  ┆ null        ┆ GATTTGGGGTTCAAAGCAGTATCGATCAAATA… ┆ !''*((((***+))%%%++)(%%%%).1***-… │
# │ SEQ_ID2 ┆ null        ┆ GATTTGGGGTTCAAAGCAGTATCGATCAAATA… ┆ !''*((((***+))%%%++)(%%%%).1***-… │
# └─────────┴─────────────┴───────────────────────────────────┴───────────────────────────────────┘

# The compression type is also inferred from the extension of the file
df = bb.FastqReader("test.fq.gz").read()
print(df.head())
# ┌─────────┬─────────────┬───────────────────────────────────┬───────────────────────────────────┐
# │ name    ┆ description ┆ sequence                          ┆ quality                           │
# │ ---     ┆ ---         ┆ ---                               ┆ ---                               │
# │ str     ┆ str         ┆ str                               ┆ str                               │
# ╞═════════╪═════════════╪═══════════════════════════════════╪═══════════════════════════════════╡
# │ SEQ_ID  ┆ null        ┆ GATTTGGGGTTCAAAGCAGTATCGATCAAATA… ┆ !''*((((***+))%%%++)(%%%%).1***-… │
# │ SEQ_ID2 ┆ null        ┆ GATTTGGGGTTCAAAGCAGTATCGATCAAATA… ┆ !''*((((***+))%%%++)(%%%%).1***-… │
# └─────────┴─────────────┴───────────────────────────────────┴───────────────────────────────────┘
```

Query an indexed VCF file:


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

## Similar Packages

Similar packages and/or inspiration for this package:

- https://github.com/abdenlab/saimin/
- https://github.com/tshauck/brrrr/
- https://github.com/natir/vcf2parquet/
- https://github.com/zaeleus/noodles/
- https://github.com/eto-ai/lance

## API Documentation

These docs are auto-generated, please file an issue if something is amiss.

<a id="vcf_reader"></a>

### vcf\_reader

VCF File Readers.

<a id="vcf_reader.VCFReader"></a>

#### VCFReader

```python
class VCFReader()
```

A VCF File Reader.

This class is used to read a VCF file and convert it to a polars DataFrame.

<a id="vcf_reader.VCFReader.__init__"></a>

##### \_\_init\_\_

```python
def __init__(path: Path)
```

Initialize the VCFReader.

**Arguments**:

- `path` _Path_ - Path to the VCF file.

<a id="vcf_reader.VCFReader.to_arrow_record_batch_reader"></a>

##### to\_arrow\_record\_batch\_reader

```python
def to_arrow_record_batch_reader() -> pa.RecordBatchReader
```

Convert the VCF reader to an arrow batch reader.

<a id="vcf_reader.VCFReader.to_arrow_scanner"></a>

##### to\_arrow\_scanner

```python
def to_arrow_scanner() -> ds.Scanner
```

Convert the VCF reader to an arrow scanner.

<a id="vcf_reader.VCFReader.read"></a>

##### read

```python
def read() -> pl.DataFrame
```

Read the VCF file and return a polars DataFrame.

<a id="vcf_reader.VCFIndexedReader"></a>

#### VCFIndexedReader

```python
class VCFIndexedReader()
```

An Indexed VCF File Reader.

This class is used to read or query an indexed VCF file and convert it to a
polars DataFrame.

<a id="vcf_reader.VCFIndexedReader.__init__"></a>

##### \_\_init\_\_

```python
def __init__(path: Path)
```

Initialize the VCFIndexedReader.

<a id="vcf_reader.VCFIndexedReader.read"></a>

##### read

```python
def read() -> pl.DataFrame
```

Read the VCF file and return a polars DataFrame.

<a id="vcf_reader.VCFIndexedReader.to_arrow_record_batch_reader"></a>

##### to\_arrow\_record\_batch\_reader

```python
def to_arrow_record_batch_reader() -> pa.RecordBatchReader
```

Convert the VCF reader to an arrow batch reader.

<a id="vcf_reader.VCFIndexedReader.to_arrow_scanner"></a>

##### to\_arrow\_scanner

```python
def to_arrow_scanner() -> ds.Scanner
```

Convert the VCF reader to an arrow scanner.

<a id="vcf_reader.VCFIndexedReader.query"></a>

##### query

```python
def query(region: str) -> pl.DataFrame
```

Query the VCF file and return a polars DataFrame.

**Arguments**:

- `region` _str_ - The region to query.

<a id="fasta_reader"></a>

### fasta\_reader

FASTA file reader.

<a id="fasta_reader.FastaReader"></a>

#### FastaReader

```python
class FastaReader()
```

<a id="fasta_reader.FastaReader.__init__"></a>

##### \_\_init\_\_

```python
def __init__(path: Path, compression: Compression = Compression.INFERRED)
```

Read a fasta file.

**Arguments**:

- `path` _Path_ - Path to the fasta file.

  Kwargs:
- `compression` _Compression_ - Compression type of the file. Defaults to
  Compression.INFERRED.

<a id="fasta_reader.FastaReader.read"></a>

##### read

```python
def read() -> pl.DataFrame
```

Read the fasta file and return a polars DataFrame.

<a id="fasta_reader.FastaReader.to_arrow_scanner"></a>

##### to\_arrow\_scanner

```python
def to_arrow_scanner() -> ds.Scanner
```

Convert the fasta reader to an arrow scanner.

<a id="fasta_reader.FastaReader.to_arrow_record_batch_reader"></a>

##### to\_arrow\_record\_batch\_reader

```python
def to_arrow_record_batch_reader() -> pa.RecordBatchReader
```

Convert the fasta reader to an arrow batch reader.

<a id="compression"></a>

### compression

Compression configuration.

<a id="compression.Compression"></a>

#### Compression

```python
class Compression(Enum)
```

Compression types for files.

<a id="compression.Compression.from_file"></a>

##### from\_file

```python
@classmethod
def from_file(cls, path: os.PathLike) -> "Compression"
```

Infer the compression type from the file extension.

<a id="compression.Compression.infer_or_use"></a>

##### infer\_or\_use

```python
def infer_or_use(path: os.PathLike) -> "Compression"
```

Infer the compression type from the file extension if needed.

<a id="__init__"></a>

### \_\_init\_\_

Main biobear package.

<a id="bam_reader"></a>

### bam\_reader

BAM File Readers.

<a id="bam_reader.BamReader"></a>

#### BamReader

```python
class BamReader()
```

A BAM File Reader.

<a id="bam_reader.BamReader.__init__"></a>

##### \_\_init\_\_

```python
def __init__(path: Path)
```

Initialize the BamReader.

**Arguments**:

- `path` _Path_ - Path to the BAM file.

<a id="bam_reader.BamReader.to_arrow_record_batch_reader"></a>

##### to\_arrow\_record\_batch\_reader

```python
def to_arrow_record_batch_reader() -> pa.RecordBatchReader
```

Convert the BAM reader to an arrow batch reader.

<a id="bam_reader.BamReader.to_arrow_scanner"></a>

##### to\_arrow\_scanner

```python
def to_arrow_scanner() -> ds.Scanner
```

Convert the BAM reader to an arrow scanner.

<a id="bam_reader.BamReader.read"></a>

##### read

```python
def read() -> pl.DataFrame
```

Read the BAM file and return a polars DataFrame.

<a id="bam_reader.BamIndexedReader"></a>

#### BamIndexedReader

```python
class BamIndexedReader()
```

An Indexed BAM File Reader.

<a id="bam_reader.BamIndexedReader.__init__"></a>

##### \_\_init\_\_

```python
def __init__(path: Path, index: Path)
```

Initialize the BamIndexedReader.

**Arguments**:

- `path` _Path_ - Path to the BAM file.
- `index` _Path_ - Path to the BAM index file.

<a id="bam_reader.BamIndexedReader.read"></a>

##### read

```python
def read() -> pl.DataFrame
```

Read the BAM file and return a polars DataFrame.

<a id="bam_reader.BamIndexedReader.to_arrow_record_batch_reader"></a>

##### to\_arrow\_record\_batch\_reader

```python
def to_arrow_record_batch_reader() -> pa.RecordBatchReader
```

Convert the BAM reader to an arrow batch reader.

<a id="bam_reader.BamIndexedReader.to_arrow_scanner"></a>

##### to\_arrow\_scanner

```python
def to_arrow_scanner() -> ds.Scanner
```

Convert the BAM reader to an arrow scanner.

<a id="bam_reader.BamIndexedReader.query"></a>

##### query

```python
def query(chrom: str, start: int, end: int) -> pl.DataFrame
```

Query the BAM file and return a polars DataFrame.

**Arguments**:

- `chrom` _str_ - The chromosome to query.
- `start` _int_ - The start position to query.
- `end` _int_ - The end position to query.

<a id="fastq_reader"></a>

### fastq\_reader

FASTQ reader.

<a id="fastq_reader.FastqReader"></a>

#### FastqReader

```python
class FastqReader()
```

<a id="fastq_reader.FastqReader.__init__"></a>

##### \_\_init\_\_

```python
def __init__(path: os.PathLike,
             compression: Compression = Compression.INFERRED)
```

Read a fastq file.

**Arguments**:

- `path` _Path_ - Path to the fastq file.

  Kwargs:
- `compression` _Compression_ - Compression type of the file. Defaults to
  Compression.INFERRED.

<a id="fastq_reader.FastqReader.read"></a>

##### read

```python
def read() -> pl.DataFrame
```

Read the fasta file and return a polars DataFrame.

<a id="fastq_reader.FastqReader.to_arrow_scanner"></a>

##### to\_arrow\_scanner

```python
def to_arrow_scanner() -> ds.Scanner
```

Convert the fasta reader to an arrow scanner.

<a id="fastq_reader.FastqReader.to_arrow_record_batch_reader"></a>

##### to\_arrow\_record\_batch\_reader

```python
def to_arrow_record_batch_reader() -> pa.RecordBatchReader
```

Convert the fasta reader to an arrow batch reader.

<a id="gff_reader"></a>

### gff\_reader

GFF File Reader.

<a id="gff_reader.GFFReader"></a>

#### GFFReader

```python
class GFFReader()
```

A GFF File Reader.

<a id="gff_reader.GFFReader.__init__"></a>

##### \_\_init\_\_

```python
def __init__(path: Path, compression: Compression = Compression.INFERRED)
```

Initialize the GFFReader.

**Arguments**:

- `path` - The path to the GFF file.

<a id="gff_reader.GFFReader.read"></a>

##### read

```python
def read() -> pl.DataFrame
```

Read the GFF file and return a polars DataFrame.

<a id="gff_reader.GFFReader.to_arrow_record_batch_reader"></a>

##### to\_arrow\_record\_batch\_reader

```python
def to_arrow_record_batch_reader() -> pa.RecordBatchReader
```

Convert the GFF reader to an arrow batch reader.

<a id="gff_reader.GFFReader.to_arrow_scanner"></a>

##### to\_arrow\_scanner

```python
def to_arrow_scanner() -> ds.Scanner
```

Convert the GFF reader to an arrow scanner.
