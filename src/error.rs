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

use arrow::error::ArrowError;
use datafusion::{error::DataFusionError, sql::sqlparser::parser::ParserError};
use exon::ExonError;
use pyo3::PyErr;

#[derive(Debug)]
pub enum BioBearError {
    IOError(String),
    Other(String),
    InvalidCompressionType(String),
    ParserError(String),
}

impl BioBearError {
    pub fn new(msg: &str) -> Self {
        Self::Other(msg.to_string())
    }
}

impl From<BioBearError> for PyErr {
    fn from(value: BioBearError) -> Self {
        match value {
            BioBearError::IOError(msg) => PyErr::new::<pyo3::exceptions::PyIOError, _>(msg),
            BioBearError::Other(msg) => PyErr::new::<pyo3::exceptions::PyIOError, _>(msg),
            BioBearError::InvalidCompressionType(msg) => {
                PyErr::new::<pyo3::exceptions::PyValueError, _>(msg)
            }
            BioBearError::ParserError(msg) => PyErr::new::<pyo3::exceptions::PyValueError, _>(msg),
        }
    }
}

impl From<DataFusionError> for BioBearError {
    fn from(value: DataFusionError) -> Self {
        match value {
            DataFusionError::IoError(msg) => Self::IOError(msg.to_string()),
            DataFusionError::ObjectStore(err) => Self::IOError(err.to_string()),
            _ => Self::Other(value.to_string()),
        }
    }
}

impl From<ExonError> for BioBearError {
    fn from(value: ExonError) -> Self {
        match value {
            ExonError::IOError(e) => BioBearError::IOError(e.to_string()),
            e => BioBearError::Other(e.to_string()),
        }
    }
}

impl From<ParserError> for BioBearError {
    fn from(value: ParserError) -> Self {
        Self::ParserError(value.to_string())
    }
}

impl From<ArrowError> for BioBearError {
    fn from(value: ArrowError) -> Self {
        Self::Other(value.to_string())
    }
}

impl From<std::io::Error> for BioBearError {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value.to_string())
    }
}

pub type BioBearResult<T> = std::result::Result<T, BioBearError>;
