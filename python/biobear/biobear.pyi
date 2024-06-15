# Copyright 2024 WHERE TRUE Technologies.
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

from pyarrow import Table, RecordBatchStreamReader
from typing import Optional
import enum

POLARS_INSTALLED = False
try:
    import polars as pl

    POLARS_INSTALLED = True
except ImportError:
    pass

class FileCompressionType(enum.Enum):
    """The type of compression used for a file."""

    GZIP = 0
    BGZIP = 1
    NONE = 2

class FastaSequenceDataType(enum.Enum):
    """How to treat the sequence data in a FASTA file."""

    UTF8 = 0
    LARGE_UTF8 = 1
    INTEGER_ENCODE_DNA = 2
    INTEGER_ENCODE_PROTEIN = 3

class CRAMReadOptions:
    """Options for reading CRAM data."""
    def __init__(
        self,
        /,
        region: Optional[str] = None,
        fasta_reference: Optional[str] = None,
    ) -> None: ...

class FCSReadOptions:
    """Options for reading FCS data."""
    def __init__(
        self,
        /,
        file_compression_type: Optional[FileCompressionType] = None,
    ) -> None: ...

class HMMDomTabReadOptions:
    """Options for reading HMM DomTab data."""
    def __init__(
        self,
        /,
        file_compression_type: Optional[FileCompressionType] = None,
    ) -> None: ...

class MzMLReadOptions:
    """Options for reading mzML data."""
    def __init__(
        self,
        /,
        file_compression_type: Optional[FileCompressionType] = None,
    ) -> None: ...

class GenBankReadOptions:
    """Options for reading GenBank data."""
    def __init__(
        self,
        /,
        file_compression_type: Optional[FileCompressionType] = None,
    ) -> None: ...

class GTFReadOptions:
    """Options for reading GTF data."""
    def __init__(
        self,
        /,
        file_compression_type: Optional[FileCompressionType] = None,
    ) -> None: ...

class FASTAReadOptions:
    """Options for reading FASTA data."""
    def __init__(
        self,
        /,
        file_extension: Optional[str] = None,
        file_compression_type: Optional[FileCompressionType] = None,
        fasta_sequence_data_type: Optional[FastaSequenceDataType] = None,
    ) -> None: ...

class FASTQReadOptions:
    """Options for reading FASTQ data."""
    def __init__(
        self,
        /,
        file_extension: Optional[str] = None,
        file_compression_type: Optional[FileCompressionType] = None,
    ) -> None: ...

class VCFReadOptions:
    """Options for reading VCF data."""
    def __init__(
        self,
        /,
        region: Optional[str] = None,
        file_compression_type: Optional[FileCompressionType] = None,
        parse_info: bool = False,
        parse_formats: bool = False,
        partition_cols: list[str | None] = None,
    ) -> None: ...

class BCFReadOptions:
    """Options for reading BCF data."""
    def __init__(
        self,
        /,
        region: Optional[str] = None,
    ) -> None: ...

class SAMReadOptions:
    """Options for reading SAM data."""
    def __init__(
        self,
    ) -> None: ...

class BAMReadOptions:
    """Options for reading BAM data."""
    def __init__(
        self,
        /,
        region: Optional[str] = None,
    ) -> None: ...

class BEDReadOptions:
    """Options for reading BED data."""
    def __init__(
        self,
        /,
        file_compression_type: Optional[FileCompressionType] = None,
        n_fields: Optional[int] = None,
        file_extension: Optional[str] = None,
    ) -> None: ...

class BigWigReadOptions:
    """Options for reading BigWig data."""
    def __init__(
        self,
        /,
        zoom: Optional[int] = None,
        region: Optional[str] = None,
    ) -> None: ...

class GFFReadOptions:
    """Options for reading GFF data."""
    def __init__(
        self,
        /,
        file_extension: Optional[str] = None,
        file_compression_type: Optional[FileCompressionType] = None,
        region: Optional[str] = None,
    ) -> None: ...

class ExecutionResult:
    """The result of an execution."""
    def to_arrow(self) -> Table:
        """Converts the result to an Arrow Table."""
    def to_arrow_record_batch_reader(self) -> RecordBatchStreamReader:
        """Converts the result to an Arrow RecordBatchStreamReader."""

    if POLARS_INSTALLED:
        def to_polars(self) -> pl.DataFrame:
            """Converts the result to a Polars DataFrame."""

class BioBearSessionContext:
    def __init__(self) -> None: ...
    def read_fastq_file(
        self, file_path: str, /, options: Optional[FASTQReadOptions] = None
    ) -> ExecutionResult:
        """Reads one or more FASTQ files and returns an ExecutionResult."""
    def read_fasta_file(
        self, file_path: str, /, options: Optional[FASTAReadOptions] = None
    ) -> ExecutionResult:
        """Reads one or more FASTA files and returns an ExecutionResult."""
    def read_vcf_file(
        self, file_path: str, /, options: Optional[VCFReadOptions] = None
    ) -> ExecutionResult:
        """Reads one or more VCF files and returns an ExecutionResult."""
    def read_bcf_file(
        self, file_path: str, /, options: Optional[BCFReadOptions] = None
    ) -> ExecutionResult:
        """Reads one or more BCF files and returns an ExecutionResult."""
    def read_sam_file(
        self, file_path: str, /, options: Optional[SAMReadOptions] = None
    ) -> ExecutionResult:
        """Reads a SAM file and returns an ExecutionResult."""
    def read_bam_file(
        self, file_path: str, /, options: Optional[BAMReadOptions] = None
    ) -> ExecutionResult:
        """Reads a BAM file and returns an ExecutionResult."""
    def read_bed_file(
        self, file_path: str, /, options: Optional[BEDReadOptions] = None
    ) -> ExecutionResult:
        """Reads a BED file and returns an ExecutionResult."""
    def read_bigwig_file(
        self, file_path: str, /, options: Optional[BigWigReadOptions] = None
    ) -> ExecutionResult:
        """Reads a BigWig file and returns an ExecutionResult."""
    def read_gff_file(
        self, file_path: str, /, options: Optional[GFFReadOptions] = None
    ) -> ExecutionResult:
        """Reads a GFF file and returns an ExecutionResult."""
    def read_gtf_file(
        self, file_path: str, /, options: Optional[GTFReadOptions] = None
    ) -> ExecutionResult:
        """Reads a GTF file and returns an ExecutionResult."""
    def read_mzml_file(
        self, file_path: str, /, options: Optional[MzMLReadOptions] = None
    ) -> ExecutionResult:
        """Reads a mzML file and returns an ExecutionResult."""
    def read_genbank_file(
        self, file_path: str, /, options: Optional[GenBankReadOptions] = None
    ) -> ExecutionResult:
        """Reads a GenBank file and returns an ExecutionResult."""
    def read_cram_file(
        self, file_path: str, /, options: Optional[CRAMReadOptions] = None
    ) -> ExecutionResult:
        """Reads a CRAM file and returns an ExecutionResult."""
    def read_fcs_file(
        self, file_path: str, /, options: Optional[FCSReadOptions] = None
    ) -> ExecutionResult:
        """Reads a FCS file and returns an ExecutionResult."""
    def sql(self, query: str) -> ExecutionResult:
        """Executes a SQL query and returns an ExecutionResult."""
    def execute(self, query: str) -> None:
        """Executes a SQL query."""

def connect() -> BioBearSessionContext:
    """Connect to the BioBear server and return a session context.

    Note:
        This function is deprecated. Use `new_session` instead.

    Returns:
        BioBearSessionContext: A session context for interacting with the BioBear server.

    """

def new_session() -> BioBearSessionContext:
    """Create a new session context for interacting with the BioBear server.

    Returns:
        BioBearSessionContext: A session context for interacting with the BioBear server.

    """
