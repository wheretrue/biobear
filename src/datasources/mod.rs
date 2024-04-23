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

use noodles::core::Region;
use pyo3::PyResult;

pub mod bam;
pub mod bcf;
pub mod bed;
pub mod bigwig;
pub mod cram;
pub mod fasta;
pub mod fastq;
pub mod fcs;
pub mod genbank;
pub mod gff;
pub mod gtf;
pub mod hmm_dom_tab;
pub mod mzml;
pub mod sam;
pub mod vcf;

pub(crate) fn parse_region(region: Option<String>) -> PyResult<Option<noodles::core::Region>> {
    let region = region
        .map(|r| Region::from_str(&r))
        .transpose()
        .map_err(|e| {
            crate::error::BioBearError::ParserError(format!("Couldn\'t parse region error {}", e))
        })?;

    Ok(region)
}
