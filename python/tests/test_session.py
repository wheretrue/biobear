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
"""Test the session context."""

from pathlib import Path
import importlib
import tempfile
import polars as pl

import pytest

from biobear import (
    BAMReadOptions,
    connect,
    FastaSequenceDataType,
    FASTQReadOptions,
    FASTAReadOptions,
    FileCompressionType,
    BEDReadOptions,
    BCFReadOptions,
    GFFReadOptions,
    VCFReadOptions,
    GTFReadOptions,
    MzMLReadOptions,
    CRAMReadOptions,
    new_session,
)

DATA = Path(__file__).parent / "data"


def test_connect_and_to_arrow():
    """Test connecting to a context."""
    session = connect()

    gff_path = DATA / "test.gff"

    query = f"CREATE EXTERNAL TABLE gff_file STORED AS GFF LOCATION '{gff_path}'"
    session.sql(query)

    query = "SELECT * FROM gff_file"
    arrow_table = session.sql(query).to_arrow()

    assert len(arrow_table) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_read_fastq_with_qs_to_list():
    """Test quality scores to list."""
    session = connect()
    fastq_path = DATA / "test.fastq.gz"

    df = session.sql(
        f"""
        SELECT quality_scores_to_list(quality_scores) quality_score_list,
            locate_regex(sequence, '[AC]AT') locate
        FROM fastq_scan('{fastq_path}')
        """
    ).to_polars()

    assert len(df) == 2
    assert df.get_column("quality_score_list").to_list()[0][:5] == [0, 6, 6, 9, 7]
    assert df.get_column("locate").to_list() == [
        [
            {"start": 29, "end": 32, "match": "AAT"},
            {"start": 36, "end": 39, "match": "AAT"},
            {"start": 40, "end": 43, "match": "CAT"},
        ],
        [
            {"start": 29, "end": 32, "match": "AAT"},
            {"start": 36, "end": 39, "match": "AAT"},
            {"start": 40, "end": 43, "match": "CAT"},
        ],
    ]


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_alignment_score():
    """Test reading a fastq file."""
    session = connect()

    df = session.sql(
        """
        SELECT s1, s2, alignment_score(s1, s2) score
        FROM (
            SELECT 'ACGT' s1, 'ACGT' s2
            UNION ALL
            SELECT 'ACG' s1, 'ACGT' s2
            UNION ALL
            SELECT 'ACGT' s1, 'ACGTT' s2
            UNION ALL
            SELECT 'ACCT' s1, 'ACGT' s2
        ) t
        ORDER BY s1, s2, score
        """
    ).to_polars()
    assert len(df) == 4

    # expected score
    score = [2, 3, 4, 4]
    assert df.get_column("score").to_list() == score


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_read_fastq():
    """Test reading a fastq file."""
    session = connect()

    fastq_path = DATA / "test.fq.gz"
    options = FASTQReadOptions(
        file_extension="fq", file_compression_type=FileCompressionType.GZIP
    )

    df = session.read_fastq_file(str(fastq_path), options=options).to_polars()

    assert len(df) == 2

    fastq_path = DATA / "test.fq"
    options = FASTQReadOptions(file_extension="fq")

    df = session.read_fastq_file(str(fastq_path), options=options).to_polars()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_read_fastq_no_options():
    """Test reading a fastq file."""
    session = connect()

    fastq_path = DATA / "test.fq.gz"
    df = session.read_fastq_file(str(fastq_path)).to_polars()

    assert len(df) == 2

    fastq_path = DATA / "test.fq"
    df = session.read_fastq_file(str(fastq_path)).to_polars()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_read_fasta():
    """Test reading a fasta file."""
    session = connect()

    fasta_path = DATA / "test.fasta"

    df = session.read_fasta_file(str(fasta_path)).to_polars()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_fasta_roundtrip():
    """Test reading a fasta file."""

    session = connect()

    fasta_path = DATA / "test.fasta"

    session.execute(
        f"CREATE EXTERNAL TABLE fasta_file STORED AS FASTA LOCATION '{fasta_path}'"
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        session.execute(f"COPY fasta_file TO '{tmpdir}/test.fasta' STORED AS FASTA")

        df = session.read_fasta_file(f"{tmpdir}/test.fasta").to_polars()

        assert len(df) == 2


@pytest.mark.skip
def test_fasta_sequence_type():
    """Test reading a fasta file."""
    session = connect()

    session.execute(
        f"""
   CREATE EXTERNAL TABLE one_hot_fasta
   STORED AS FASTA
   OPTIONS (fasta_sequence_data_type 'integer_encode_dna')
   LOCATION '{DATA / "test.fasta"}'
   """
    )

    df = session.sql("SELECT * FROM one_hot_fasta").to_polars()

    assert df.get_column("sequence").dtype == pl.List(pl.Int8)


def test_fasta_sequence_type_with_options():
    """Test reading a fasta file."""
    session = connect()

    df = session.read_fasta_file(
        str(DATA / "test.fasta"),
        options=FASTAReadOptions(
            fasta_sequence_data_type=FastaSequenceDataType.INTEGER_ENCODE_DNA
        ),
    ).to_polars()

    assert df.get_column("sequence").dtype == pl.List(pl.Int8)


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_read_fasta_fa():
    """Test reading a fasta file."""
    session = connect()

    fasta_path = DATA / "test.fa"

    options = FASTAReadOptions(file_extension="fa")
    df = session.read_fasta_file(str(fasta_path), options=options).to_polars()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_read_fasta_fa_no_options():
    """Test reading a fasta file."""
    session = connect()

    # Test reading a fasta file with no options no compression
    fasta_path = DATA / "test.fa"
    df = session.read_fasta_file(str(fasta_path)).to_polars()

    assert len(df) == 2

    # Test reading a fasta file with no options with compression
    fasta_path = DATA / "test.fa.gz"
    df = session.read_fasta_file(str(fasta_path)).to_polars()

    # Test conflicting options
    fasta_path = DATA / "test.fa"
    df = session.read_fasta_file(
        str(fasta_path),
        options=FASTAReadOptions(file_extension="fasta"),
    ).to_polars()

    assert len(df) == 0


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_read_fasta_gz():
    """Test reading a fasta file."""
    session = connect()

    fasta_path = DATA / "test.fa.gz"

    options = FASTAReadOptions(
        file_extension="fa", file_compression_type=FileCompressionType.GZIP
    )
    df = session.read_fasta_file(str(fasta_path), options=options).to_polars()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_to_polars():
    """Test converting to a polars dataframe."""
    session = connect()

    gff_path = DATA / "test.gff"

    query = f"CREATE EXTERNAL TABLE gff_file STORED AS GFF LOCATION '{gff_path}'"
    session.sql(query)

    query = "SELECT * FROM gff_file"
    df = session.sql(query).to_polars()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_to_polars_empty():
    """Test converting to a polars dataframe when the query is empty."""

    session = connect()

    fasta_file = DATA / "test.fasta"
    query = f"SELECT * FROM fasta_scan('{fasta_file}') WHERE id = 'not found'"

    results = session.sql(query)
    df = results.to_polars()
    assert len(df) == 0


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_indexed_scan():
    """Test an indexed fasta can works."""

    session = connect()

    fasta_file = DATA / "test.fasta"
    query = f"SELECT * FROM fasta_indexed_scan('{fasta_file}', 'a:1-2')"

    results = session.sql(query)
    df = results.to_polars()

    assert len(df) == 1, df


def test_with_error():
    """Test what happens on a bad query."""
    session = connect()

    query = "SELECT * FROM gff_file"
    with pytest.raises(Exception):
        session.sql(query)


def test_execute(tmp_path):
    """Test the execute query returns immediately."""

    output_path = tmp_path / "output.parquet"

    session = connect()

    gff_path = DATA / "test.gff"

    query = f"CREATE EXTERNAL TABLE gff_file STORED AS GFF LOCATION '{gff_path}'"
    session.execute(query)

    copy_query = f"COPY (SELECT seqname FROM gff_file) TO '{output_path}'"
    session.execute(copy_query)

    assert output_path.exists()


def test_to_record_batch_reader():
    """Test converting to a record batch reader."""
    session = connect()

    gff_path = DATA / "test.gff"

    query = f"CREATE EXTERNAL TABLE gff_file STORED AS GFF LOCATION '{gff_path}'"
    session.sql(query)

    query = "SELECT * FROM gff_file"
    reader = session.sql(query).to_arrow_record_batch_reader()

    rows = 0
    for batch in reader:
        rows += len(batch)

    assert rows == 2


def test_read_from_s3():
    """Test reading from s3."""
    session = connect()

    query = "SELECT * FROM fasta_scan('s3://test-bucket/test.fasta')"
    arrow_table = session.sql(query).to_arrow()

    assert len(arrow_table) == 2


def test_copy_to_s3():
    """Test copying to s3."""
    session = connect()

    s3_input_path = "s3://test-bucket/test.fasta"
    parquet_output = "s3://parquet-bucket/test.parquet"

    query = f"COPY (SELECT * FROM fasta_scan('{s3_input_path}')) TO '{parquet_output}'"

    session.register_object_store_from_url(parquet_output)

    # Should not raise an exception
    session.execute(query)


def test_read_bcf_file():
    """Test reading a BCF file."""
    session = connect()

    bcf_path = DATA / "index.bcf"

    arrow_table = session.read_bcf_file(bcf_path.as_posix()).to_arrow()

    assert len(arrow_table) == 621


def test_bcf_indexed_reader_query():
    """Test the BCFIndexedReader.query() method."""
    session = connect()
    options = BCFReadOptions(region="1")

    bcf_path = DATA / "index.bcf"

    rbr = session.read_bcf_file(
        bcf_path.as_posix(), options=options
    ).to_arrow_record_batch_reader()

    assert 191 == sum(b.num_rows for b in rbr)


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_vcf_reader():
    session = connect()
    options = VCFReadOptions()

    df = session.read_vcf_file(
        (DATA / "vcf_file.vcf").as_posix(), options=options
    ).to_polars()

    assert len(df) == 15


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_vcf_reader_with_parsing():
    session = new_session()
    options = VCFReadOptions(parse_formats=True, parse_info=True)

    df = session.read_vcf_file(
        (DATA / "vcf_file.vcf").as_posix(), options=options
    ).to_polars()

    # Check that this is a struct, with three fields
    assert len(df.get_column("info").dtype.fields) == 6
    assert len(df) == 15


def test_vcf_query_with_region_and_partition():
    session = connect()
    options = VCFReadOptions(
        region="1",
        file_compression_type=FileCompressionType.GZIP,
        parse_formats=True,
        parse_info=True,
        partition_cols=["sample"],
    )

    rbr = session.read_vcf_file(
        (DATA / "vcf-partition").as_posix(), options=options
    ).to_arrow_record_batch_reader()

    batch = next(rbr)

    assert batch.column_names == [
        "chrom",
        "pos",
        "id",
        "ref",
        "alt",
        "qual",
        "filter",
        "info",
        "formats",
        "sample",
    ]

    batch.schema.field("sample")

    assert 11 == batch.num_rows


def test_vcf_query():
    session = connect()
    options = VCFReadOptions(region="1", file_compression_type=FileCompressionType.GZIP)

    rbr = session.read_vcf_file(
        (DATA / "vcf_file.vcf.gz").as_posix(), options=options
    ).to_arrow_record_batch_reader()

    assert 11 == sum(b.num_rows for b in rbr)

    options = VCFReadOptions(
        region="chr1", file_compression_type=FileCompressionType.GZIP
    )

    rbr = session.read_vcf_file(
        (DATA / "vcf_file.vcf.gz").as_posix(), options=options
    ).to_arrow_record_batch_reader()

    assert 0 == sum(b.num_rows for b in rbr)


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_read_bam():
    session = new_session()

    df = session.read_bam_file((DATA / "bedcov.bam").as_posix()).to_polars()

    assert len(df) == 61


def test_bam_indexed_reader():
    session = new_session()
    options = BAMReadOptions(region="chr1:12203700-12205426")

    rbr = session.read_bam_file(
        (DATA / "bedcov.bam").as_posix(), options=options
    ).to_arrow_record_batch_reader()

    assert 1 == sum(b.num_rows for b in rbr)


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_gff_reader_polars():
    session = new_session()
    df = session.read_gff_file((DATA / "test.gff").as_posix()).to_polars()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_gff_attr_struct():
    session = new_session()

    reader = session.read_gff_file((DATA / "test.gff").as_posix())
    df = reader.to_polars()
    dtype = df.select(pl.col("attributes")).dtypes[0]

    key_field = pl.Field("key", pl.Utf8)
    value_field = pl.Field("value", pl.List(pl.Utf8))
    assert dtype == pl.List(pl.Struct([key_field, value_field]))


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_gff_reader_gz():
    session = new_session()

    options = GFFReadOptions(file_compression_type=FileCompressionType.GZIP)

    reader = session.read_gff_file((DATA / "test.gff.gz").as_posix(), options=options)
    df = reader.to_polars()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_gff_reader_no_options():
    session = new_session()

    reader = session.read_gff_file((DATA / "test.gff.gz").as_posix())
    df = reader.to_polars()

    assert len(df) == 2

    reader = session.read_gff_file((DATA / "test.gff3.gz").as_posix())
    df = reader.to_polars()

    assert len(df) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_gtf_reader_to_polars():
    session = new_session()

    df = session.read_gtf_file((DATA / "test.gtf").as_posix()).to_polars()

    assert len(df) == 77


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_gtf_attr_struct():
    session = new_session()

    result = session.read_gtf_file((DATA / "test.gtf").as_posix())
    df = result.to_polars()

    dtype = df.select(pl.col("attributes")).dtypes[0]

    key_field = pl.Field("key", pl.Utf8)
    value_field = pl.Field("value", pl.Utf8)
    assert dtype == pl.List(pl.Struct([key_field, value_field]))


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_gtf_reader_gz_to_polars():
    session = new_session()
    options = GTFReadOptions(file_compression_type=FileCompressionType.GZIP)

    result = session.read_gtf_file((DATA / "test.gtf.gz").as_posix(), options=options)

    assert len(result.to_polars()) == 77


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_read_mzml_file():
    session = new_session()

    reader = session.read_mzml_file((DATA / "test.mzML").as_posix())

    assert len(reader.to_polars()) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_mzml_reader_gz():
    session = new_session()

    options = MzMLReadOptions(file_compression_type=FileCompressionType.GZIP)
    df = session.read_mzml_file((DATA / "test.mzML.gz").as_posix(), options=options)

    assert len(df.to_polars()) == 2


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_genbank_reader():
    session = new_session()

    result = session.read_genbank_file((DATA / "BGC0000404.gbk").as_posix())
    df = result.to_polars()

    assert len(df) == 1


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_cram_reader():
    session = new_session()

    result = session.read_cram_file((DATA / "cram" / "test_input_1_a.cram").as_posix())

    assert len(result.to_polars()) == 15


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_cram_reader_with_region():
    session = new_session()

    fasta_reference = (DATA / "two-cram" / "rand1k.fa").as_posix()
    options = CRAMReadOptions(region="1", fasta_reference=fasta_reference)

    result = session.read_cram_file(
        (DATA / "two-cram" / "twolib.sorted.cram").as_posix(),
        options=options,
    )

    assert len(result.to_polars()) == 0


def test_bed_reader():
    session = new_session()

    bed_file = DATA / "test.bed"
    result = session.read_bed_file(bed_file.as_posix())

    assert result.to_polars().shape == (10, 12)


def test_bed_reader_no_options():
    session = new_session()

    bed_file = DATA / "test-three.bedd"
    result = session.read_bed_file(
        bed_file.as_posix(), options=BEDReadOptions(n_fields=3)
    )

    assert result.to_polars().shape == (10, 3)

    bed_file = DATA / "test-three.bedd"
    result = session.read_bed_file(bed_file.as_posix())

    # 12 here because the bed file has 12 fields by default
    assert result.to_polars().shape == (10, 12)


def test_bed_three():
    session = new_session()

    bed_file = DATA / "test-three.bed"
    options = BEDReadOptions(n_fields=3)
    result = session.read_bed_file(bed_file.as_posix(), options=options)

    assert result.to_polars().shape == (10, 3)


def test_bed_four():
    session = new_session()

    bed_file = DATA / "test-four.bed"
    options = BEDReadOptions(n_fields=4)
    result = session.read_bed_file(bed_file.as_posix(), options=options)

    assert result.to_polars().shape == (10, 4)


def test_cripri_example():
    session = new_session()

    fasta_file = DATA / "example_crispri_v2_sample.fastq.gz"
    result = session.sql(
        f"SELECT name, COUNT(*) FROM fastq_scan('{fasta_file}') GROUP BY name"
    ).to_polars()

    assert len(result) == 25000


def test_sdf_file():
    session = new_session()

    sdf_file = DATA / "tox_benchmark_N6512.sdf"
    result = session.read_sdf_file(sdf_file.as_posix())

    assert len(result.to_polars()) == 4


def test_sdf_gzip_file():
    session = new_session()

    sdf_file = DATA / "tox_benchmark_N6512.sdf.gz"
    result = session.read_sdf_file(sdf_file.as_posix())

    assert len(result.to_polars()) == 4


def test_bed_long_name():
    session = new_session()

    bed_file = DATA / "name_256bytes.one.bed"

    df = session.read_bed_file(bed_file.as_posix()).to_polars()

    assert len(df) == 1

    # Check the value of the long name
    assert (
        df.get_column("name")[0]
        == "PURK_peak_11,INH_SST_peak_18b,INH_SST_peak_18c,GC_peak_40a,GC_peak_40b,GC_peak_40c,OPC_peak_30b,OPC_peak_30c,MOL_peak_32c,INH_VIP_peak_18a,NFOL_peak_32e,NFOL_peak_32f,AST_CER_peak_48e,AST_CER_peak_48f,ENDO_peak_7,AST_peak_17c,GP_peak_20,ASTP_peak_19e,INH_V"
    )
