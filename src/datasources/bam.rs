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

use exon::datasources::bam::table_provider::ListingBAMTableOptions;
use noodles::core::Region;
use pyo3::{pyclass, pymethods, PyResult};

use super::parse_region;

#[pyclass]
#[derive(Debug, Clone, Default)]
pub struct BAMReadOptions {
    region: Option<Region>,
}

#[pymethods]
impl BAMReadOptions {
    #[new]
    #[pyo3(signature = (region=None))]
    pub fn try_new(region: Option<String>) -> PyResult<Self> {
        let region = parse_region(region)?;

        Ok(Self { region })
    }
}

impl From<BAMReadOptions> for ListingBAMTableOptions {
    fn from(options: BAMReadOptions) -> Self {
        let regions = options.region.map(|r| vec![r]).unwrap_or_default();

        let mut t = ListingBAMTableOptions::default();

        if !regions.is_empty() {
            t = t.with_regions(regions);
        }

        t
    }
}
