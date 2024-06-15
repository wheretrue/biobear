# Copyright 2023 WHERE TRUE Technologies.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Main biobear package."""

from biobear.fasta_reader import FastaReader
from biobear.fastq_reader import FastqReader
from biobear.vcf_reader import VCFReader, VCFIndexedReader
from biobear.bam_reader import BamReader, BamIndexedReader
from biobear.gtf_reader import GTFReader
from biobear.gff_reader import GFFReader
from biobear.mzml_reader import MzMLReader
from biobear.genbank_reader import GenbankReader
from biobear.bcf_reader import BCFReader, BCFIndexedReader
from biobear import compression
from biobear.compression import Compression

from .biobear import FileCompressionType
from .biobear import FASTQReadOptions
from .biobear import FASTAReadOptions
from .biobear import VCFReadOptions
from .biobear import BCFReadOptions
from .biobear import BEDReadOptions
from .biobear import BigWigReadOptions
from .biobear import SAMReadOptions
from .biobear import BAMReadOptions
from .biobear import GFFReadOptions
from .biobear import GTFReadOptions
from .biobear import HMMDomTabReadOptions
from .biobear import MzMLReadOptions
from .biobear import GenBankReadOptions
from .biobear import FCSReadOptions
from .biobear import CRAMReadOptions
from .biobear import connect
from .biobear import new_session
from .biobear import __runtime


__version__ = "0.22.0"

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
    "MzMLReader",
    "compression",
    "Compression",
    "FileCompressionType",
    "FASTQReadOptions",
    "FASTAReadOptions",
    "BCFReadOptions",
    "VCFReadOptions",
    "BEDReadOptions",
    "FCSReadOptions",
    "CRAMReadOptions",
    "BigWigReadOptions",
    "SAMReadOptions",
    "BAMReadOptions",
    "GenBankReadOptions",
    "GFFReadOptions",
    "GTFReadOptions",
    "MzMLReadOptions",
    "HMMDomTabReadOptions",
    "__version__",
    "connect",
    "new_session",
    "__runtime",
]
