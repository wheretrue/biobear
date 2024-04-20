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

use crate::FileCompressionType;

#[pyclass]
#[derive(Debug, Clone, Default)]
pub struct HMMDomTabReadOptions {
    file_compression_type: FileCompressionType,
}

#[pymethods]
impl HMMDomTabReadOptions {
    #[new]
    fn new(file_compression_type: Option<FileCompressionType>) -> Self {
        Self {
            file_compression_type: file_compression_type.unwrap_or_default(),
        }
    }
}

impl From<HMMDomTabReadOptions> for ListingHMMDomTabTableOptions {
    fn from(options: HMMDomTabReadOptions) -> Self {
        ListingHMMDomTabTableOptions::new(options.file_compression_type.into())
    }
}
