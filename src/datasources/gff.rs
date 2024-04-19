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

use exon::datasources::gff::table_provider::ListingGFFTableOptions;
use noodles::core::Region;
use pyo3::{pyclass, pymethods, PyResult};

use crate::FileCompressionType;

use super::parse_region;

#[pyclass]
#[derive(Debug, Clone)]
pub struct GFFReadOptions {
    region: Option<Region>,
    file_compression_type: FileCompressionType,
}

impl Default for GFFReadOptions {
    fn default() -> Self {
        Self {
            region: None,
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
}

#[pymethods]
impl GFFReadOptions {
    #[new]
    #[pyo3(signature = (/, region = None, file_compression_type = None))]
    fn try_new(
        region: Option<String>,
        file_compression_type: Option<FileCompressionType>,
    ) -> PyResult<Self> {
        let region = parse_region(region)?;
        Ok(Self {
            region,
            file_compression_type: file_compression_type
                .unwrap_or(FileCompressionType::UNCOMPRESSED),
        })
    }
}

impl From<GFFReadOptions> for ListingGFFTableOptions {
    fn from(options: GFFReadOptions) -> Self {
        let mut o = ListingGFFTableOptions::new(options.file_compression_type.into());

        if let Some(region) = options.region {
            o = o.with_region(region);
        }

        o
    }
}
