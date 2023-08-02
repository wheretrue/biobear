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

"""Tests for the BCFReader and BCFIndexedReader classes."""

from pathlib import Path
import importlib

import pytest

from biobear import BCFReader, BCFIndexedReader

DATA = Path(__file__).parent / "data"


@pytest.mark.skipif(
    not importlib.util.find_spec("polars"), reason="polars not installed"
)
def test_bcf_reader():
    """Test the BCFReader."""
    reader = BCFReader(DATA / "index.bcf")
    df = reader.to_polars()

    assert len(df) == 621


# Add test for to_pandas() method
@pytest.mark.skipif(
    not importlib.util.find_spec("pandas"), reason="pandas not installed"
)
def test_bcf_reader_to_pandas():
    """Test the BCFReader."""
    reader = BCFReader(DATA / "index.bcf")
    df = reader.to_pandas()

    assert len(df) == 621


def test_bcf_reader_missing_file():
    """Test the BCFReader with a missing file."""
    with pytest.raises(OSError):
        BCFReader("test.bcf")


def test_bcf_indexed_reader_query():
    """Test the BCFIndexedReader.query() method."""
    reader = BCFIndexedReader(DATA / "index.bcf")
    rbr = reader.query("1")

    assert 191 == sum(b.num_rows for b in rbr)


def test_bcf_indexed_reader_query_no_results():
    reader = BCFIndexedReader(DATA / "index.bcf")
    rbr = reader.query("1:100000000-100000001")

    with pytest.raises(Exception):
        next(rbr)
