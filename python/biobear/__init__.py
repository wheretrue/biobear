"""Main biobear package."""

from biobear.fasta_reader import FastaReader
from biobear.fastq_reader import FastqReader
from biobear.vcf_reader import VCFReader, VCFIndexedReader
from biobear.bam_reader import BamReader, BamIndexedReader
from biobear.gtf_reader import GTFReader
from biobear.gff_reader import GFFReader
from biobear.genbank_reader import GenbankReader
from biobear.bcf_reader import BCFReader, BCFIndexedReader

from biobear import compression

__all__ = [
    "FastaReader",
    "FastqReader",
    "VCFReader",
    "VCFIndexedReader",
    "BamReader",
    "BamIndexedReader",
    "BCFReader",
    "BCFIndexedReader",
    "GFFReader",
    "GTFReader",
    "GenbankReader",
    "compression",
]
