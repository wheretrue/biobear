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

use crate::{error::BioBearResult, file_options::FileOptions, FileCompressionType};

#[pyclass]
#[derive(Debug, Clone, Default)]
/// Options for reading SDF files.
pub struct SDFReadOptions {
    file_compression_type: Option<FileCompressionType>,
}

#[pymethods]
impl SDFReadOptions {
    #[new]
    #[pyo3(signature = (/, file_compression_type=None))]
    /// Create a new SDFReadOptions instance.
    pub fn new(file_compression_type: Option<FileCompressionType>) -> Self {
        Self {
            file_compression_type,
        }
    }
}

impl SDFReadOptions {
    pub(crate) fn update_from_file_options(
        &mut self,
        file_options: &FileOptions,
    ) -> BioBearResult<()> {
        if let Some(file_compression_type) = file_options.file_compression_type() {
            let fct = FileCompressionType::try_from(file_compression_type)?;
            self.file_compression_type = Some(fct);
        }

        Ok(())
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
