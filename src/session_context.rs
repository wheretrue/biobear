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

use exon::datasources::bigwig;
use exon::{ExonRuntimeEnvExt, ExonSession};

use pyo3::prelude::*;

use crate::datasources::bcf::BCFReadOptions;
use crate::datasources::bigwig::BigWigReadOptions;
use crate::datasources::fasta::FASTAReadOptions;
use crate::datasources::fastq::FASTQReadOptions;
use crate::datasources::hmm_dom_tab::HMMDomTabReadOptions;
use crate::datasources::mzml::MzMLReadOptions;
use crate::error;
use crate::execution_result::ExecutionResult;
use crate::runtime::wait_for_future;

#[pyclass]
pub struct BioBearSessionContext {
    ctx: ExonSession,
}

impl Default for BioBearSessionContext {
    fn default() -> Self {
        Self {
            ctx: ExonSession::new_exon(),
        }
    }
}

#[pymethods]
impl BioBearSessionContext {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Self::default())
    }

    /// Read one or more VCF files from the given path.
    fn read_vcf_file(
        &mut self,
        file_path: &str,
        options: Option<crate::datasources::vcf::VCFReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_vcf(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a HMM Dom Tab file from the given path.
    fn read_hmm_dom_tab_file(
        &mut self,
        file_path: &str,
        options: Option<HMMDomTabReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_hmm_dom_tab(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a bigwig file from the given path.
    fn read_bigwig_file(
        &mut self,
        file_path: &str,
        options: Option<BigWigReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        match options.zoom() {
            Some(_) => {
                let options = bigwig::zoom::ListingTableOptions::try_from(options)?;
                let result = self.ctx.read_bigwig_zoom(file_path, options);
                let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

                Ok(ExecutionResult::new(df))
            }
            None => {
                let options = bigwig::value::ListingTableOptions::try_from(options)?;

                let result = self.ctx.read_bigwig_view(file_path, options);
                let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

                Ok(ExecutionResult::new(df))
            }
        }
    }

    /// Read a gff file from the given path.
    fn read_gff_file(
        &mut self,
        file_path: &str,
        options: Option<crate::datasources::gff::GFFReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_gff(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a fastq file from the given path.
    fn read_fastq_file(
        &mut self,
        file_path: &str,
        options: Option<FASTQReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_fastq(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a genbank file from the given path.
    fn read_genbank_file(
        &mut self,
        file_path: &str,
        options: Option<crate::datasources::genbank::GenBankReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_genbank(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a CRAM file from the given path.
    fn read_cram_file(
        &mut self,
        file_path: &str,
        options: Option<crate::datasources::cram::CRAMReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_cram(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a mzml file from the given path.
    fn read_mzml_file(
        &mut self,
        file_path: &str,
        options: Option<MzMLReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_mzml(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a GTF file from the given path.
    fn read_gtf_file(
        &mut self,
        file_path: &str,
        options: Option<crate::datasources::gtf::GTFReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_gtf(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a BCF file from the given path.
    fn read_bcf_file(
        &mut self,
        file_path: &str,
        options: Option<BCFReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_bcf(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a fasta file from the given path.
    fn read_fasta_file(
        &mut self,
        file_path: &str,
        options: Option<FASTAReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_fasta(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a BED file from the given path.
    fn read_bed_file(
        &mut self,
        file_path: &str,
        options: Option<crate::datasources::bed::BEDReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_bed(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a BAM file from the given path.
    fn read_bam_file(
        &mut self,
        file_path: &str,
        options: Option<crate::datasources::bam::BAMReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_bam(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Read a SAM file from the given path.
    fn read_sam_file(
        &mut self,
        file_path: &str,
        options: Option<crate::datasources::sam::SAMReadOptions>,
        py: Python,
    ) -> PyResult<ExecutionResult> {
        let options = options.unwrap_or_default();

        let result = self.ctx.read_sam(file_path, options.into());
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Generate the plan from a SQL query and return the result as a [`PyExecutionResult`].
    fn sql(&mut self, query: &str, py: Python) -> PyResult<ExecutionResult> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(ExecutionResult::new(df))
    }

    /// Execute the SQL query eagerly, but do not collect the results.
    fn execute(&mut self, query: &str, py: Python) -> PyResult<()> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        wait_for_future(py, df.collect()).map_err(error::BioBearError::from)?;

        Ok(())
    }

    /// Register an object store with the given URL.
    fn register_object_store_from_url(&mut self, url: &str, py: Python) -> PyResult<()> {
        let runtime = self.ctx.session.runtime_env();
        let registration = runtime.exon_register_object_store_uri(url);
        wait_for_future(py, registration).map_err(error::BioBearError::from)?;

        Ok(())
    }
}

#[pyfunction]
pub fn connect() -> PyResult<BioBearSessionContext> {
    Ok(BioBearSessionContext::default())
}

#[pyfunction]
pub fn new_session() -> PyResult<BioBearSessionContext> {
    Ok(BioBearSessionContext::default())
}
