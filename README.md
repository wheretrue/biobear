<h1 align="center">
    <img src=".github/biobear.svg" width="400px" alt="biobear" />
</h1>

biobear is a Python library designed for reading and searching bioinformatic file formats, using Rust as its backend and producing Arrow Batch Readers and other downstream formats (like polars or duckdb).

The python package has minimal dependencies and only requires Polars. Biobear can be used to read various bioinformatic file formats, including FASTA, FASTQ, VCF, BAM, and GFF locally or from an object store like S3. It can also query some indexed file formats locally like VCF and BAM.

[![Release](https://github.com/wheretrue/biobear/actions/workflows/release.yml/badge.svg)](https://github.com/wheretrue/biobear/actions/workflows/release.yml)

Please see the [documentation] for information on how to get started using biobear.

[documentation]: https://www.wheretrue.dev/docs/exon/biobear/.
