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
use exon::ExonSessionExt;

use pyo3::prelude::*;

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

    fn sql(&mut self, query: &str, py: Python) -> PyResult<PyExecutionResult> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result).map_err(error::BioBearError::from)?;

        Ok(PyExecutionResult::new(df))
    }
}

#[pyfunction]
pub fn connect() -> PyResult<ExonSessionContext> {
    Ok(ExonSessionContext::default())
}
