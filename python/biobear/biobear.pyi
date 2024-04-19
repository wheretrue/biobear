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
    GZIP = 0
    BGZIP = 1
    NONE = 2

class FASTAReadOptions:
    def __init__(
        self,
        /,
        file_extension: Optional[str] = None,
        file_compression_type: Optional[FileCompressionType] = None,
    ) -> None: ...

class FASTQReadOptions:
    def __init__(
        self,
        /,
        file_extension: Optional[str] = None,
        file_compression_type: Optional[FileCompressionType] = None,
    ) -> None: ...

class VCFReadOptions:
    def __init__(
        self,
        /,
        region: Optional[str] = None,
        file_extension: Optional[str] = None,
        file_compression_type: Optional[FileCompressionType] = None,
    ) -> None: ...

class BCFReadOptions:
    def __init__(
        self,
        /,
        region: Optional[str] = None,
    ) -> None: ...

class SAMReadOptions:
    def __init__(
        self,
    ) -> None: ...

class BAMReadOptions:
    def __init__(
        self,
        /,
        region: Optional[str] = None,
    ) -> None: ...

class BEDReadOptions:
    def __init__(
        self,
    ) -> None: ...

class BigWigReadOptions:
    def __init__(
        self,
        /,
        zoom: Optional[int] = None,
        region: Optional[str] = None,
    ) -> None: ...

class GFFReadOptions:
    def __init__(
        self,
        /,
        file_extension: Optional[str] = None,
        file_compression_type: Optional[FileCompressionType] = None,
        region: Optional[str] = None,
    ) -> None: ...

class ExecutionResult:
    def to_arrow(self) -> Table: ...
    def to_arrow_record_batch_reader(self) -> RecordBatchStreamReader: ...

    if POLARS_INSTALLED:
        def to_polars(self) -> pl.DataFrame: ...

class BioBearSessionContext:
    def __init__(self) -> None: ...
    def read_fastq_file(
        self, file_path: str, /, options: Optional[FASTQReadOptions]
    ) -> ExecutionResult: ...
    def read_fasta_file(
        self, file_path: str, /, options: Optional[FASTAReadOptions]
    ) -> ExecutionResult: ...
    def read_vcf_file(
        self, file_path: str, /, options: Optional[VCFReadOptions]
    ) -> ExecutionResult: ...
    def read_bcf_file(
        self, file_path: str, /, options: Optional[BCFReadOptions]
    ) -> ExecutionResult: ...
    def read_sam_file(
        self, file_path: str, /, options: Optional[SAMReadOptions]
    ) -> ExecutionResult: ...
    def read_bam_file(
        self, file_path: str, /, options: Optional[BAMReadOptions]
    ) -> ExecutionResult: ...
    def read_bed_file(
        self, file_path: str, /, options: Optional[BEDReadOptions]
    ) -> ExecutionResult: ...
    def read_bigwig_file(
        self, file_path: str, /, options: Optional[BigWigReadOptions]
    ) -> ExecutionResult: ...
    def read_gff_file(
        self, file_path: str, /, options: Optional[GFFReadOptions]
    ) -> ExecutionResult: ...
    def sql(self, query: str) -> ExecutionResult: ...
    def execute(self, query: str) -> None: ...

def connect() -> BioBearSessionContext: ...
def new_session() -> BioBearSessionContext: ...
