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

use crate::{file_options::impl_settable_from_file_options, FileCompressionType};

#[pyclass]
#[derive(Debug, Clone, Default)]
/// Options for reading BED files.
pub struct BEDReadOptions {
    /// The type of compression used in the file.
    file_compression_type: Option<FileCompressionType>,

    /// The number of fields in the file.
    n_fields: Option<usize>,

    /// The file extension.
    file_extension: Option<String>,
}

impl_settable_from_file_options!(BEDReadOptions);

#[pymethods]
impl BEDReadOptions {
    #[new]
    #[pyo3(signature = (/, file_compression_type = None, n_fields = None, file_extension = None))]
    fn new(
        file_compression_type: Option<FileCompressionType>,
        n_fields: Option<usize>,
        file_extension: Option<String>,
    ) -> Self {
        Self {
            file_compression_type,
            n_fields,
            file_extension,
        }
    }
}

impl From<BEDReadOptions> for ListingBEDTableOptions {
    fn from(options: BEDReadOptions) -> Self {
        let file_compression_type = options
            .file_compression_type
            .unwrap_or(FileCompressionType::UNCOMPRESSED);
        let n_fields = options.n_fields.unwrap_or(12);
        let file_extension = options.file_extension.unwrap_or_default();

        ListingBEDTableOptions::new(file_compression_type.into())
            .with_n_fields(n_fields)
            .with_file_extension(file_extension)
    }
}
