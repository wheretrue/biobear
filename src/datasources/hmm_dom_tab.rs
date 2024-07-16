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

use exon::datasources::hmmdomtab::table_provider::ListingHMMDomTabTableOptions;
use pyo3::{pyclass, pymethods};

use crate::{file_options::SettableFromFileOptions, FileCompressionType};

const DEFAULT_HMM_FILE_EXTENSION: &str = "hmmdomtab";

#[pyclass]
#[derive(Debug, Clone, Default)]
pub struct HMMDomTabReadOptions {
    file_compression_type: Option<FileCompressionType>,
    file_extension: Option<String>,
}

impl SettableFromFileOptions for HMMDomTabReadOptions {
    fn file_extension_mut(&mut self) -> &mut Option<String> {
        &mut self.file_extension
    }

    fn file_compression_type_mut(&mut self) -> &mut Option<FileCompressionType> {
        &mut self.file_compression_type
    }
}

#[pymethods]
impl HMMDomTabReadOptions {
    #[new]
    fn new(
        file_extension: Option<String>,
        file_compression_type: Option<FileCompressionType>,
    ) -> Self {
        Self {
            file_extension,
            file_compression_type,
        }
    }
}

impl HMMDomTabReadOptions {}

impl From<HMMDomTabReadOptions> for ListingHMMDomTabTableOptions {
    fn from(options: HMMDomTabReadOptions) -> Self {
        let file_compression_type = options
            .file_compression_type
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        let file_extension = options
            .file_extension
            .unwrap_or(DEFAULT_HMM_FILE_EXTENSION.to_string());

        ListingHMMDomTabTableOptions::new(file_compression_type.into())
            .with_file_extension(file_extension)
    }
}
