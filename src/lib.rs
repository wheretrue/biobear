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

mod runtime;

mod bam_reader;
mod bcf_reader;
mod datasources;
mod exon_reader;
mod vcf_reader;

mod file_compression_type;

pub use file_compression_type::FileCompressionType;

pub(crate) mod error;
mod execution_result;
mod session_context;

use std::sync::atomic::{AtomicU64, Ordering};

use pyo3::prelude::*;
use runtime::TokioRuntime;
use tokio::runtime::Builder;

#[pymodule]
fn biobear(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let runtime = Builder::new_multi_thread()
        .thread_name_fn(move || {
            static THREAD_ID: AtomicU64 = AtomicU64::new(0);
            let id = THREAD_ID.fetch_add(1, Ordering::Relaxed);
            format!("biobear-python-thread-{}", id)
        })
        .enable_all()
        .build()?;

    m.add("__runtime", TokioRuntime(runtime))?;

    m.add_class::<exon_reader::ExonReader>()?;

    m.add_class::<bam_reader::BamIndexedReader>()?;
    m.add_class::<vcf_reader::VCFIndexedReader>()?;
    m.add_class::<bcf_reader::BCFIndexedReader>()?;
    m.add_class::<file_compression_type::FileCompressionType>()?;
    m.add_class::<datasources::fastq::FASTQReadOptions>()?;
    m.add_class::<datasources::fasta::FASTAReadOptions>()?;
    m.add_class::<datasources::bcf::BCFReadOptions>()?;
    m.add_class::<datasources::vcf::VCFReadOptions>()?;
    m.add_class::<datasources::bam::BAMReadOptions>()?;
    m.add_class::<datasources::sam::SAMReadOptions>()?;
    m.add_class::<datasources::bed::BEDReadOptions>()?;
    m.add_class::<datasources::gff::GFFReadOptions>()?;
    m.add_class::<datasources::gtf::GTFReadOptions>()?;
    m.add_class::<datasources::bigwig::BigWigReadOptions>()?;
    m.add_class::<datasources::mzml::MzMLReadOptions>()?;
    m.add_class::<datasources::hmm_dom_tab::HMMDomTabReadOptions>()?;
    m.add_class::<datasources::genbank::GenBankReadOptions>()?;
    m.add_class::<datasources::cram::CRAMReadOptions>()?;
    m.add_class::<datasources::fcs::FCSReadOptions>()?;

    m.add_function(wrap_pyfunction!(session_context::connect, m)?)?;
    m.add_function(wrap_pyfunction!(session_context::new_session, m)?)?;

    Ok(())
}
