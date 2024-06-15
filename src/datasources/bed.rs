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
    /// The type of compression used in the file.
    file_compression_type: crate::FileCompressionType,

    /// The number of fields in the file.
    n_fields: usize,

    /// The file extension.
    file_extension: String,
}

impl Default for BEDReadOptions {
    fn default() -> Self {
        Self {
            file_compression_type: crate::FileCompressionType::UNCOMPRESSED,
            n_fields: 12,
            file_extension: "bed".to_string(),
        }
    }
}

#[pymethods]
impl BEDReadOptions {
    #[new]
    #[pyo3(signature = (/, file_compression_type = None, n_fields = None, file_extension = None))]
    fn new(
        file_compression_type: Option<crate::FileCompressionType>,
        n_fields: Option<usize>,
        file_extension: Option<String>,
    ) -> Self {
        Self {
            file_compression_type: file_compression_type
                .unwrap_or(crate::FileCompressionType::UNCOMPRESSED),
            n_fields: n_fields.unwrap_or(12),
            file_extension: file_extension.unwrap_or("bed".to_string()),
        }
    }
}

impl From<BEDReadOptions> for ListingBEDTableOptions {
    fn from(options: BEDReadOptions) -> Self {
        ListingBEDTableOptions::new(options.file_compression_type.into())
            .with_n_fields(options.n_fields)
            .with_file_extension(options.file_extension)
    }
}
