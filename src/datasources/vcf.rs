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

use arrow::datatypes::{DataType, Field};
use exon::datasources::vcf::ListingVCFTableOptions;
use noodles::core::Region;
use pyo3::{pyclass, pymethods, PyResult};

use crate::FileCompressionType;

use super::parse_region;

#[pyclass]
#[derive(Debug, Clone, Default)]
/// Options for reading VCF files.
pub struct VCFReadOptions {
    /// The region to read.
    region: Option<Region>,
    /// The file compression type.
    file_compression_type: FileCompressionType,
    /// True if the INFO column should be parsed.
    parse_info: bool,
    /// True if the FORMAT column should be parsed.
    parse_formats: bool,
    /// The partition fields.
    partition_cols: Option<Vec<String>>,
}

#[pymethods]
impl VCFReadOptions {
    #[new]
    #[pyo3(signature = (*, region = None, file_compression_type = None, parse_info = false, parse_formats = false, partition_cols = None))]
    fn try_new(
        region: Option<String>,
        file_compression_type: Option<FileCompressionType>,
        parse_info: bool,
        parse_formats: bool,
        partition_cols: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let region = parse_region(region)?;

        let file_compression_type =
            file_compression_type.unwrap_or(FileCompressionType::UNCOMPRESSED);

        Ok(Self {
            region,
            file_compression_type,
            parse_info,
            parse_formats,
            partition_cols,
        })
    }
}

impl From<VCFReadOptions> for ListingVCFTableOptions {
    fn from(options: VCFReadOptions) -> Self {
        let mut o = ListingVCFTableOptions::new(options.file_compression_type.into(), false)
            .with_parse_info(options.parse_info)
            .with_parse_formats(options.parse_formats);

        let regions = options.region.map(|r| vec![r]).unwrap_or_default();
        if !regions.is_empty() {
            o = o.with_regions(regions);
        }

        if let Some(partition_cols) = options.partition_cols {
            let partition_fields = partition_cols
                .iter()
                .map(|s| Field::new(s, DataType::Utf8, false))
                .collect::<Vec<_>>();

            o = o.with_table_partition_cols(partition_fields);
        }

        o
    }
}
