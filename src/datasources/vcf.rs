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

use std::str::FromStr;

use exon::datasources::vcf::ListingVCFTableOptions;
use noodles::core::Region;
use pyo3::{pyclass, pymethods, PyResult};

use crate::FileCompressionType;

#[pyclass]
#[derive(Debug, Clone)]
/// Options for reading VCF files.
pub struct VCFReadOptions {
    region: Option<Region>,
    file_compression_type: FileCompressionType,
}

impl Default for VCFReadOptions {
    fn default() -> Self {
        Self {
            region: None,
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
}

#[pymethods]
impl VCFReadOptions {
    #[new]
    #[pyo3(signature = (/, region = None, file_compression_type = None))]
    fn try_new(
        region: Option<String>,
        file_compression_type: Option<FileCompressionType>,
    ) -> PyResult<Self> {
        let region = region
            .map(|r| Region::from_str(&r))
            .transpose()
            .map_err(|e| {
                crate::error::BioBearError::ParserError(format!(
                    "Couldn\'t parse region error {}",
                    e
                ))
            })?;

        let file_compression_type =
            file_compression_type.unwrap_or(FileCompressionType::UNCOMPRESSED);

        Ok(Self {
            region,
            file_compression_type,
        })
    }
}

impl From<VCFReadOptions> for ListingVCFTableOptions {
    fn from(options: VCFReadOptions) -> Self {
        let regions = options.region.map(|r| vec![r]).unwrap_or_default();

        let mut t = ListingVCFTableOptions::new(options.file_compression_type.into(), false);

        if !regions.is_empty() {
            t = t.with_regions(regions);
        }

        t
    }
}
