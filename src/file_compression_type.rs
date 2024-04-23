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

use std::{fmt::Display, str::FromStr};

use datafusion::{
    common::parsers::CompressionTypeVariant,
    datasource::file_format::file_compression_type::FileCompressionType as DFFileCompressionType,
};
use pyo3::prelude::*;

use crate::error::BioBearError;

#[pyclass]
#[derive(Debug, Clone)]
pub enum FileCompressionType {
    GZIP,
    ZSTD,
    UNCOMPRESSED,
}

impl Default for FileCompressionType {
    fn default() -> Self {
        Self::UNCOMPRESSED
    }
}

#[pymethods]
impl FileCompressionType {
    #[new]
    fn new(s: &str) -> PyResult<Self> {
        let s = FileCompressionType::from_str(s)?;

        Ok(s)
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

impl Display for FileCompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GZIP => write!(f, "GZIP"),
            Self::ZSTD => write!(f, "ZSTD"),
            Self::UNCOMPRESSED => write!(f, "UNCOMPRESSED"),
        }
    }
}

impl From<FileCompressionType> for DFFileCompressionType {
    fn from(value: FileCompressionType) -> Self {
        match value {
            FileCompressionType::GZIP => DFFileCompressionType::GZIP,
            FileCompressionType::ZSTD => DFFileCompressionType::ZSTD,
            FileCompressionType::UNCOMPRESSED => DFFileCompressionType::UNCOMPRESSED,
        }
    }
}

impl TryFrom<CompressionTypeVariant> for FileCompressionType {
    type Error = BioBearError;

    fn try_from(value: CompressionTypeVariant) -> Result<Self, Self::Error> {
        match value {
            CompressionTypeVariant::GZIP => Ok(Self::GZIP),
            CompressionTypeVariant::ZSTD => Ok(Self::ZSTD),
            CompressionTypeVariant::UNCOMPRESSED => Ok(Self::UNCOMPRESSED),
            _ => Err(BioBearError::InvalidCompressionType(value.to_string())),
        }
    }
}

impl TryFrom<DFFileCompressionType> for FileCompressionType {
    type Error = BioBearError;

    fn try_from(value: DFFileCompressionType) -> Result<Self, Self::Error> {
        match value {
            DFFileCompressionType::GZIP => Ok(Self::GZIP),
            DFFileCompressionType::ZSTD => Ok(Self::ZSTD),
            DFFileCompressionType::UNCOMPRESSED => Ok(Self::UNCOMPRESSED),
            _ => Err(BioBearError::InvalidCompressionType(
                "Invalid compression type".to_string(),
            )),
        }
    }
}

impl FromStr for FileCompressionType {
    type Err = BioBearError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = CompressionTypeVariant::from_str(s)?;

        Self::try_from(v)
    }
}
