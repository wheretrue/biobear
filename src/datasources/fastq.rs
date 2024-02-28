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

// #[derive(Debug, Clone)]
// /// Listing options for a FASTQ table
// pub struct ListingFASTQTableOptions {
//     /// The file extension for the table
//     file_extension: String,

//     /// The file compression type
//     file_compression_type: FileCompressionType,

//     /// The table partition columns
//     table_partition_cols: Vec<Field>,
// }

use crate::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType as DFFileCompressionType;
use exon::datasources::fastq::table_provider::ListingFASTQTableOptions;
use pyo3::{pyclass, pymethods};

#[pyclass]
#[derive(Debug, Clone)]
pub struct FASTQReadOptions {
    file_extension: String,
    file_compression_type: DFFileCompressionType,
}

impl Default for FASTQReadOptions {
    fn default() -> Self {
        Self {
            file_extension: "fastq".to_string(),
            file_compression_type: DFFileCompressionType::UNCOMPRESSED,
        }
    }
}

#[pymethods]
impl FASTQReadOptions {
    #[new]
    pub fn new(file_extension: String, file_compression_type: FileCompressionType) -> Self {
        Self {
            file_extension,
            file_compression_type: file_compression_type.into(),
        }
    }
}

impl Into<ListingFASTQTableOptions> for FASTQReadOptions {
    fn into(self) -> ListingFASTQTableOptions {
        ListingFASTQTableOptions::new(self.file_compression_type)
            .with_file_extension(self.file_extension)
    }
}
