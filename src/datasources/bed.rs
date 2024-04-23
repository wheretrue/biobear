// Copyright 2024 WHERE TRUE Technologies.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use exon::datasources::bed::table_provider::ListingBEDTableOptions;
use pyo3::{pyclass, pymethods};

#[pyclass]
#[derive(Debug, Clone)]
/// Options for reading BED files.
pub struct BEDReadOptions {
    file_compression_type: crate::FileCompressionType,
}

impl Default for BEDReadOptions {
    fn default() -> Self {
        Self {
            file_compression_type: crate::FileCompressionType::UNCOMPRESSED,
        }
    }
}

#[pymethods]
impl BEDReadOptions {
    #[new]
    #[pyo3(signature = (/, file_compression_type = None))]
    fn new(file_compression_type: Option<crate::FileCompressionType>) -> Self {
        Self {
            file_compression_type: file_compression_type
                .unwrap_or(crate::FileCompressionType::UNCOMPRESSED),
        }
    }
}

impl From<BEDReadOptions> for ListingBEDTableOptions {
    fn from(options: BEDReadOptions) -> Self {
        ListingBEDTableOptions::new(options.file_compression_type.into())
    }
}
