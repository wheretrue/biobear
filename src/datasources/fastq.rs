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

use crate::{
    error::BioBearResult, file_compression_type::FileCompressionType, file_options::FileOptions,
};
use exon::datasources::fastq::table_provider::ListingFASTQTableOptions;
use pyo3::{pyclass, pymethods};

const DEFAULT_FASTQ_FILE_EXTENSION: &str = "fastq";

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
#[pyclass]
#[derive(Debug, Clone, Default)]
pub struct FASTQReadOptions {
    file_extension: Option<String>,
    file_compression_type: Option<FileCompressionType>,
}

#[pymethods]
impl FASTQReadOptions {
    #[new]
    #[pyo3(signature = (/, file_extension=None, file_compression_type=None))]
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
    ) -> Self {
        Self {
            file_extension,
            file_compression_type,
        }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }
}

impl FASTQReadOptions {
    pub(crate) fn update_from_file_options(
        &mut self,
        file_options: &FileOptions,
    ) -> BioBearResult<()> {
        if let Some(file_extension) = file_options.file_extension() {
            self.file_extension = Some(file_extension.to_string());
        }

        if let Some(file_compression_type) = file_options.file_compression_type() {
            let fct = FileCompressionType::try_from(file_compression_type)?;
            self.file_compression_type = Some(fct);
        }

        Ok(())
    }
}

impl From<FASTQReadOptions> for ListingFASTQTableOptions {
    fn from(options: FASTQReadOptions) -> Self {
        let file_compression_type = options
            .file_compression_type
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        let file_extension = options
            .file_extension
            .as_deref()
            .unwrap_or(DEFAULT_FASTQ_FILE_EXTENSION);

        ListingFASTQTableOptions::new(file_compression_type.into())
            .with_some_file_extension(Some(file_extension))
    }
}
