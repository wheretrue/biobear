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

from pathlib import Path
import importlib

import pytest

from biobear import connect

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
def test_to_polars():
    """Test converting to a polars dataframe."""
    session = connect()

    gff_path = DATA / "test.gff"

    query = f"CREATE EXTERNAL TABLE gff_file STORED AS GFF LOCATION '{gff_path}'"
    session.sql(query)

    query = "SELECT * FROM gff_file"
    df = session.sql(query).to_polars()

    assert len(df) == 2


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

    copy_query = (
        f"COPY (SELECT seqname FROM gff_file) TO '{output_path}' (FORMAT PARQUET)"
    )
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

    query = f"COPY (SELECT * FROM fasta_scan('{s3_input_path}')) TO '{parquet_output}' (FORMAT PARQUET)"

    session.register_object_store_from_url(parquet_output)

    # Should not raise an exception
    session.execute(query)
