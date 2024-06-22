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

use crate::{error::BioBearResult, file_options::FileOptions, FileCompressionType};

use super::parse_region;

#[pyclass]
#[derive(Debug, Clone, Default)]
pub struct GFFReadOptions {
    region: Option<Region>,
    file_extension: Option<String>,
    file_compression_type: Option<FileCompressionType>,
}

#[pymethods]
impl GFFReadOptions {
    #[new]
    #[pyo3(signature = (/, region = None, file_compression_type = None, file_extension=None))]
    fn try_new(
        region: Option<String>,
        file_compression_type: Option<FileCompressionType>,
        file_extension: Option<String>,
    ) -> PyResult<Self> {
        let region = parse_region(region)?;
        Ok(Self {
            region,
            file_compression_type,
            file_extension,
        })
    }
}

impl GFFReadOptions {
    pub(crate) fn update_from_file_options(&mut self, options: &FileOptions) -> BioBearResult<()> {
        if let Some(file_extension) = options.file_extension() {
            if self.file_extension.is_none() {
                self.file_extension = Some(file_extension.to_string());
            }
        }

        if let Some(file_compression_type) = options.file_compression_type() {
            if self.file_compression_type.is_none() {
                let fct = FileCompressionType::try_from(file_compression_type)?;
                self.file_compression_type = Some(fct);
            }
        }

        Ok(())
    }
}

impl From<GFFReadOptions> for ListingGFFTableOptions {
    fn from(options: GFFReadOptions) -> Self {
        let file_compression_type = options
            .file_compression_type
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        let file_extension = options.file_extension.unwrap_or("gff".to_string());

        let mut o = ListingGFFTableOptions::new(file_compression_type.into())
            .with_file_extension(Some(file_extension));

        if let Some(region) = options.region {
            o = o.with_region(region);
        }

        o
    }
}
