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

use exon::datasources::cram;
use noodles::core::Region;
use pyo3::{pyclass, pymethods, PyResult};

use super::parse_region;

#[pyclass]
#[derive(Debug, Clone, Default)]
pub struct CRAMReadOptions {
    region: Option<Region>,
    fasta_reference: Option<String>,
}

#[pymethods]
impl CRAMReadOptions {
    #[new]
    pub fn try_new(region: Option<String>, fasta_reference: Option<String>) -> PyResult<Self> {
        let region = parse_region(region)?;

        Ok(Self {
            region,
            fasta_reference,
        })
    }
}

impl From<CRAMReadOptions> for cram::table_provider::ListingCRAMTableOptions {
    fn from(options: CRAMReadOptions) -> Self {
        let mut t = cram::table_provider::ListingCRAMTableOptions::default()
            .with_fasta_reference(options.fasta_reference);

        if let Some(region) = options.region {
            t = t.with_region(Some(region)).with_indexed(true);
        }

        t
    }
}
