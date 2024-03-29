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

use datafusion::prelude::SessionContext;
use exon::{ExonRuntimeEnvExt, ExonSessionExt};

use pyo3::prelude::*;

use crate::datasources::fasta::FASTAReadOptions;
use crate::datasources::fastq::FASTQReadOptions;
use crate::error;
use crate::execution_result::PyExecutionResult;
use crate::runtime::wait_for_future;

#[pyclass]
pub struct ExonSessionContext {
    ctx: SessionContext,
}

impl Default for ExonSessionContext {
    fn default() -> Self {
        Self {
            ctx: SessionContext::new_exon(),
        }
    }
}

#[pymethods]
impl ExonSessionContext {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Self::default())
    }

    /// Read a fastq file from the given path.
    #[pyo3(signature = (file_path, *, options=None))]
    fn read_fastq_file(
        &mut self,
        file_path: &str,
        options: Option<FASTQReadOptions>,
        py: Python,
    ) -> PyResult<PyExecutionResult> {
        let options = options.map(|o| o.into());

        let result = self.ctx.read_fastq(file_path, options);
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(PyExecutionResult::new(df))
    }

    /// Read a fasta file from the given path.
    #[pyo3(signature = (file_path, *, options=None))]
    fn read_fasta_file(
        &mut self,
        file_path: &str,
        options: Option<FASTAReadOptions>,
        py: Python,
    ) -> PyResult<PyExecutionResult> {
        let options = options.map(|o| o.into());

        let result = self.ctx.read_fasta(file_path, options);
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(PyExecutionResult::new(df))
    }

    /// Generate the plan from a SQL query and return the result as a [`PyExecutionResult`].
    fn sql(&mut self, query: &str, py: Python) -> PyResult<PyExecutionResult> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(PyExecutionResult::new(df))
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
        let runtime = self.ctx.runtime_env();
        let registration = runtime.exon_register_object_store_uri(url);
        wait_for_future(py, registration).map_err(error::BioBearError::from)?;

        Ok(())
    }
}

#[pyfunction]
pub fn connect() -> PyResult<ExonSessionContext> {
    Ok(ExonSessionContext::default())
}
