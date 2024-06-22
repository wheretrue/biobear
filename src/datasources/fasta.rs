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
use exon::datasources::fasta::{table_provider::ListingFASTATableOptions, SequenceDataType};
use pyo3::{pyclass, pymethods};

const DEFAULT_FASTA_FILE_EXTENSION: &str = "fasta";

#[derive(Debug, Clone)]
#[pyclass]
pub enum FastaSequenceDataType {
    UTF8,
    #[allow(non_camel_case_types)]
    LARGE_UTF8,
    #[allow(non_camel_case_types)]
    INTEGER_ENCODE_DNA,
    #[allow(non_camel_case_types)]
    INTEGER_ENCODE_PROTEIN,
}

impl From<FastaSequenceDataType> for SequenceDataType {
    fn from(data_type: FastaSequenceDataType) -> Self {
        match data_type {
            FastaSequenceDataType::UTF8 => SequenceDataType::Utf8,
            FastaSequenceDataType::LARGE_UTF8 => SequenceDataType::LargeUtf8,
            FastaSequenceDataType::INTEGER_ENCODE_DNA => SequenceDataType::IntegerEncodeDNA,
            FastaSequenceDataType::INTEGER_ENCODE_PROTEIN => SequenceDataType::IntegerEncodeProtein,
        }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
/// Options for reading FASTA files.
///
/// When using from Python, the arguments are optional, but if passed, must be passed as kwargs.
///
/// ```python
/// from exon import FASTAReadOptions
///
/// # Create a new FASTAReadOptions instance with the default values.
/// options = FASTAReadOptions()
///
/// # Create a new FASTAReadOptions instance with the given file extension and file compression type.
/// options = FASTAReadOptions(file_extension="fa", file_compression_type=FileCompressionType.GZIP)
/// ```
///
/// # Examples
///
/// Create a new FASTAReadOptions instance with the default values.
///
/// ```rust
/// use exon::datasources::fasta::FASTAReadOptions;
///
/// let options = FASTAReadOptions::default();
/// assert_eq!(options.file_extension, "fasta");
/// ```
#[derive(Default)]
pub struct FASTAReadOptions {
    file_extension: Option<String>,
    file_compression_type: Option<FileCompressionType>,
    fasta_sequence_data_type: Option<FastaSequenceDataType>,
}

#[pymethods]
impl FASTAReadOptions {
    #[new]
    #[pyo3(signature = (/, file_extension=None, file_compression_type=None, fasta_sequence_data_type=None))]
    /// Create a new FASTAReadOptions instance.
    ///
    /// # Arguments
    ///
    /// * `file_extension` - The file extension to use for the FASTA file.
    /// * `file_compression_type` - The file compression type to use for the FASTA file.
    ///
    /// # Returns
    ///
    /// A new FASTAReadOptions instance.
    ///
    /// # Note
    ///
    /// The arguments are optional in Python, but if passed, must be passed as kwargs.
    pub fn new(
        file_extension: Option<String>,
        file_compression_type: Option<FileCompressionType>,
        fasta_sequence_data_type: Option<FastaSequenceDataType>,
    ) -> Self {
        Self {
            file_compression_type,
            file_extension,
            fasta_sequence_data_type,
        }
    }
}

impl FASTAReadOptions {
    pub(crate) fn update_from_file_options(
        &mut self,
        file_options: &FileOptions,
    ) -> BioBearResult<()> {
        if let Some(file_extension) = file_options.file_extension() {
            if self.file_extension.is_none() {
                self.file_extension = Some(file_extension.to_string());
            }
        }

        if let Some(file_compression_type) = file_options.file_compression_type() {
            if self.file_compression_type.is_none() {
                let fct = FileCompressionType::try_from(file_compression_type)?;
                self.file_compression_type = Some(fct);
            }
        }

        Ok(())
    }
}

impl From<FASTAReadOptions> for ListingFASTATableOptions {
    fn from(options: FASTAReadOptions) -> Self {
        let file_compression_type = options
            .file_compression_type
            .unwrap_or(FileCompressionType::UNCOMPRESSED);
        let fasta_sequence_data_type = options
            .fasta_sequence_data_type
            .unwrap_or(FastaSequenceDataType::UTF8);
        let file_extension = options
            .file_extension
            .unwrap_or(DEFAULT_FASTA_FILE_EXTENSION.to_string());

        ListingFASTATableOptions::new(file_compression_type.into())
            .with_sequence_data_type(fasta_sequence_data_type.into())
            .with_some_file_extension(Some(&file_extension))
    }
}
