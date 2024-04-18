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

class ExecutionResult:
    def to_arrow(self) -> Table: ...
    def to_arrow_record_batch(self) -> RecordBatchStreamReader: ...

    if POLARS_INSTALLED:
        def to_polars(self) -> pl.DataFrame: ...

class BioBearSessionContext:
    def __init__(self) -> None: ...
    def read_fastq_file(
        self, file_path: str, options: FASTQReadOptions
    ) -> ExecutionResult: ...
    def read_fasta_file(
        self, file_path: str, options: FASTAReadOptions
    ) -> ExecutionResult: ...
    def sql(self, query: str) -> ExecutionResult: ...
    def execute(self, query: str) -> None: ...

def connect() -> BioBearSessionContext: ...
def new_session() -> BioBearSessionContext: ...
