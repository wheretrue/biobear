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

use exon::datasources::mzml::table_provider::ListingMzMLTableOptions;
use pyo3::{pyclass, pymethods};

use crate::{file_options::impl_settable_from_file_options, FileCompressionType};

#[pyclass]
#[derive(Debug, Clone)]
/// Options for reading mzML files.
pub struct MzMLReadOptions {
    file_compression_type: Option<FileCompressionType>,
    file_extension: Option<String>,
}

impl_settable_from_file_options!(MzMLReadOptions);

impl Default for MzMLReadOptions {
    fn default() -> Self {
        Self {
            file_compression_type: Some(FileCompressionType::UNCOMPRESSED),
            file_extension: None,
        }
    }
}

#[pymethods]
impl MzMLReadOptions {
    #[new]
    fn new(file_compression_type: Option<FileCompressionType>) -> Self {
        Self {
            file_compression_type: Some(
                file_compression_type.unwrap_or(FileCompressionType::UNCOMPRESSED),
            ),
            file_extension: None,
        }
    }
}

impl From<MzMLReadOptions> for ListingMzMLTableOptions {
    fn from(options: MzMLReadOptions) -> Self {
        let file_compression_type = options
            .file_compression_type
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        let mut new_options = ListingMzMLTableOptions::new(file_compression_type.into());

        // let file_extension = options.file_extension;
        if let Some(fe) = options.file_extension {
            eprintln!("Setting file extension to {}", fe);
            new_options = new_options.with_file_extension(fe)
        }

        new_options
    }
}
