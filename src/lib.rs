// Copyright 2023 WHERE TRUE Technologies.
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

use pyo3::prelude::*;

mod bam_reader;
mod bcf_reader;
mod exon_reader;
mod vcf_reader;

#[pymodule]
fn biobear(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<exon_reader::ExonReader>()?;

    m.add_class::<bam_reader::BamIndexedReader>()?;
    m.add_class::<vcf_reader::VCFIndexedReader>()?;
    m.add_class::<bcf_reader::BCFIndexedReader>()?;

    Ok(())
}
