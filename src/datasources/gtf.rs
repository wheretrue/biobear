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

use exon::datasources::gtf::table_provider::ListingGTFTableOptions;
use pyo3::{pyclass, pymethods};

use crate::{file_options::impl_settable_from_file_options, FileCompressionType};

#[pyclass]
#[derive(Debug, Clone)]
pub struct GTFReadOptions {
    file_compression_type: Option<FileCompressionType>,
    file_extension: Option<String>,
}

impl Default for GTFReadOptions {
    fn default() -> Self {
        Self {
            file_compression_type: Some(FileCompressionType::UNCOMPRESSED),
            file_extension: None,
        }
    }
}

impl_settable_from_file_options!(GTFReadOptions);

#[pymethods]
impl GTFReadOptions {
    #[new]
    #[pyo3(signature = (file_compression_type=None))]
    pub fn new(file_compression_type: Option<FileCompressionType>) -> Self {
        Self {
            file_compression_type,
            file_extension: Some("gtf".to_string()),
        }
    }
}

impl From<GTFReadOptions> for ListingGTFTableOptions {
    fn from(options: GTFReadOptions) -> Self {
        ListingGTFTableOptions::new(
            options
                .file_compression_type
                .map(|c| c.into())
                .unwrap_or(datafusion::datasource::file_format::file_compression_type::FileCompressionType::UNCOMPRESSED),
        )
    }
}
