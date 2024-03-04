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

use crate::{error::BioBearResult, file_compression_type::FileCompressionType};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType as DFFileCompressionType;
use exon::datasources::fastq::table_provider::ListingFASTQTableOptions;
use pyo3::{pyclass, pymethods};

const DEFAULT_FASTQ_FILE_EXTENSION: &str = "fastq";

#[pyclass]
#[derive(Debug, Clone)]
/// Options for reading FASTQ files.
///
/// When using from Python, the arguments are optional, but if passed, must be passed as kwargs.
///
/// ```python
/// from exon import FASTQReadOptions
///
/// # Create a new FASTQReadOptions instance with the default values.
/// options = FASTQReadOptions()
///
/// # Create a new FASTQReadOptions instance with the given file extension and file compression type.
/// options = FASTQReadOptions(file_extension="fq", file_compression_type=FileCompressionType.GZIP)
/// ```
///
/// # Examples
///
/// Create a new FASTQReadOptions instance with the default values.
///
/// ```rust
/// use exon::datasources::fastq::FASTQReadOptions;
///
/// let options = FASTQReadOptions::default();
/// assert_eq!(options.file_extension, "fastq");
/// ```
pub struct FASTQReadOptions {
    file_extension: String,
    file_compression_type: DFFileCompressionType,
}

impl Default for FASTQReadOptions {
    fn default() -> Self {
        Self {
            file_extension: DEFAULT_FASTQ_FILE_EXTENSION.to_string(),
            file_compression_type: DFFileCompressionType::UNCOMPRESSED,
        }
    }
}

#[pymethods]
impl FASTQReadOptions {
    #[new]
    #[pyo3(signature = (*, file_extension=None, file_compression_type=None))]
    /// Create a new FASTQReadOptions instance.
    ///
    /// # Arguments
    ///
    /// * `file_extension` - The file extension to use for the FASTQ file.
    /// * `file_compression_type` - The file compression type to use for the FASTQ file.
    ///
    /// # Returns
    ///
    /// A new FASTQReadOptions instance.
    ///
    /// # Note
    ///
    /// The arguments are optional in Python, but if passed, must be passed as kwargs.
    pub fn new(
        file_extension: Option<String>,
        file_compression_type: Option<FileCompressionType>,
    ) -> BioBearResult<Self> {
        let file_compression_type = file_compression_type
            .unwrap_or(FileCompressionType::UNCOMPRESSED)
            .try_into()?;

        let file_extension = file_extension.unwrap_or(DEFAULT_FASTQ_FILE_EXTENSION.to_string());

        Ok(Self {
            file_extension,
            file_compression_type,
        })
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

impl From<FASTQReadOptions> for ListingFASTQTableOptions {
    fn from(options: FASTQReadOptions) -> Self {
        ListingFASTQTableOptions::new(options.file_compression_type)
            .with_file_extension(options.file_extension)
    }
}
