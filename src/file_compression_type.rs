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

use datafusion::datasource::file_format::file_compression_type::FileCompressionType as DFFileCompressionType;
use pyo3::{pyclass, pymethods};

use crate::error::{BioBearError, BioBearResult};

#[pyclass]
#[derive(Debug, Clone)]
pub struct FileCompressionType {
    inner: DFFileCompressionType,
}

#[pymethods]
impl FileCompressionType {
    #[new]
    fn from_str(compression_string: &str) -> BioBearResult<Self> {
        let inner = DFFileCompressionType::from_str(compression_string)
            .map_err(|e| BioBearError::new(&format!("Invalid compression type: {}", e)))?;

        Ok(Self { inner })
    }
}

impl From<DFFileCompressionType> for FileCompressionType {
    fn from(inner: DFFileCompressionType) -> Self {
        Self { inner }
    }
}

impl Into<DFFileCompressionType> for FileCompressionType {
    fn into(self) -> DFFileCompressionType {
        self.inner
    }
}
