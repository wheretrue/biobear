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
