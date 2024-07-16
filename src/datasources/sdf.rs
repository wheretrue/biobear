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

use exon::datasources::sdf::ListingSDFTableOptions;
use pyo3::{pyclass, pymethods};

use crate::{file_options::SettableFromFileOptions, FileCompressionType};

#[pyclass]
#[derive(Debug, Clone, Default)]
/// Options for reading SDF files.
pub struct SDFReadOptions {
    file_compression_type: Option<FileCompressionType>,
    file_extension: Option<String>,
}

impl SettableFromFileOptions for SDFReadOptions {
    fn file_extension_mut(&mut self) -> &mut Option<String> {
        &mut self.file_extension
    }

    fn file_compression_type_mut(&mut self) -> &mut Option<FileCompressionType> {
        &mut self.file_compression_type
    }
}

#[pymethods]
impl SDFReadOptions {
    #[new]
    #[pyo3(signature = (/, file_compression_type=None))]
    /// Create a new SDFReadOptions instance.
    pub fn new(file_compression_type: Option<FileCompressionType>) -> Self {
        Self {
            file_compression_type,
            file_extension: Some("sdf".to_string()),
        }
    }
}

impl From<SDFReadOptions> for ListingSDFTableOptions {
    fn from(options: SDFReadOptions) -> Self {
        let mut listing_options = ListingSDFTableOptions::default();

        if let Some(file_compression_type) = options.file_compression_type {
            listing_options =
                listing_options.with_file_compression_type(file_compression_type.into());
        }

        listing_options
    }
}
