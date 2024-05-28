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
use exon::datasources::fasta::{table_provider::ListingFASTATableOptions, SequenceDataType};
use pyo3::{pyclass, pymethods};

const DEFAULT_FASTA_FILE_EXTENSION: &str = "fasta";

#[derive(Debug, Clone)]
#[pyclass]
pub enum FastaSequenceDataType {
    Utf8,
    LargeUtf8,
    IntegerEncodeDNA,
    IntegerEncodeProtein,
}

impl From<FastaSequenceDataType> for SequenceDataType {
    fn from(data_type: FastaSequenceDataType) -> Self {
        match data_type {
            FastaSequenceDataType::Utf8 => SequenceDataType::Utf8,
            FastaSequenceDataType::LargeUtf8 => SequenceDataType::LargeUtf8,
            FastaSequenceDataType::IntegerEncodeDNA => SequenceDataType::IntegerEncodeDNA,
            FastaSequenceDataType::IntegerEncodeProtein => SequenceDataType::IntegerEncodeProtein,
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
pub struct FASTAReadOptions {
    file_extension: String,
    file_compression_type: FileCompressionType,
    fasta_sequence_data_type: FastaSequenceDataType,
}

impl Default for FASTAReadOptions {
    fn default() -> Self {
        Self {
            file_extension: String::from(DEFAULT_FASTA_FILE_EXTENSION),
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            fasta_sequence_data_type: FastaSequenceDataType::Utf8,
        }
    }
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
    ) -> BioBearResult<Self> {
        let file_compression_type =
            file_compression_type.unwrap_or(FileCompressionType::UNCOMPRESSED);

        let fasta_sequence_data_type =
            fasta_sequence_data_type.unwrap_or(FastaSequenceDataType::Utf8);

        Ok(Self {
            file_compression_type,
            file_extension: file_extension.unwrap_or(DEFAULT_FASTA_FILE_EXTENSION.to_string()),
            fasta_sequence_data_type,
        })
    }
}

impl From<FASTAReadOptions> for ListingFASTATableOptions {
    fn from(options: FASTAReadOptions) -> Self {
        ListingFASTATableOptions::new(options.file_compression_type.into())
            .with_sequence_data_type(options.fasta_sequence_data_type.into())
            .with_some_file_extension(Some(&options.file_extension))
    }
}
