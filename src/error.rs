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

use datafusion::error::DataFusionError;
use pyo3::PyErr;

#[derive(Debug, thiserror::Error)]
pub enum BioBearError {
    #[error("{0}")]
    Other(String),
}

impl BioBearError {
    pub fn new(msg: &str) -> Self {
        Self::Other(msg.to_string())
    }
}

impl From<BioBearError> for PyErr {
    fn from(value: BioBearError) -> Self {
        match value {
            BioBearError::Other(msg) => PyErr::new::<pyo3::exceptions::PyValueError, _>(msg),
        }
    }
}

impl From<DataFusionError> for BioBearError {
    fn from(value: DataFusionError) -> Self {
        Self::Other(value.to_string())
    }
}
