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

use std::{path::Path, str::FromStr};

use datafusion::datasource::file_format::file_compression_type::FileCompressionType;

use crate::error::BioBearResult;

mod settable_from_file_options;
pub(crate) use settable_from_file_options::SettableFromFileOptions;

#[derive(Debug, Clone, Default)]
pub(crate) struct FileOptions {
    file_extension: Option<String>,
    file_compression_type: Option<FileCompressionType>,
}

impl FileOptions {
    pub fn file_extension(&self) -> Option<&str> {
        self.file_extension.as_deref()
    }

    pub fn file_compression_type(&self) -> Option<FileCompressionType> {
        self.file_compression_type
    }

    pub fn set_from_file_options(
        &mut self,
        settable: &mut dyn settable_from_file_options::SettableFromFileOptions,
    ) -> BioBearResult<()> {
        if let Some(file_extension) = self.file_extension() {
            let file_options = settable.file_extension_mut();
            *file_options = Some(file_extension.to_string());
        }

        if let Some(file_compression_type) = self.file_compression_type() {
            let file_options = settable.file_compression_type_mut();
            *file_options = Some(crate::FileCompressionType::try_from(file_compression_type)?);
        }

        Ok(())
    }
}

impl From<&str> for FileOptions {
    fn from(s: &str) -> Self {
        let path = Path::new(s);

        let extension = match path.extension().and_then(|ext| ext.to_str()) {
            Some(ext) => ext,
            None => return Self::default(),
        };

        if let Ok(file_compression_type) = FileCompressionType::from_str(extension) {
            if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
                let file_extension = Path::new(stem).extension().and_then(|ext| ext.to_str());
                return Self {
                    file_extension: file_extension.map(|ext| ext.to_string()),
                    file_compression_type: Some(file_compression_type),
                };
            }
            return Self {
                file_extension: None,
                file_compression_type: Some(file_compression_type),
            };
        }

        Self {
            file_extension: Some(extension.to_string()),
            file_compression_type: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str() {
        let file_options = FileOptions::from("test.csv");
        assert_eq!(file_options.file_extension(), Some("csv"));
        assert_eq!(file_options.file_compression_type(), None);

        let file_options = FileOptions::from("test.csv.gz");
        assert_eq!(file_options.file_extension(), Some("csv"));
        assert_eq!(
            file_options.file_compression_type(),
            Some(FileCompressionType::GZIP)
        );

        let file_options = FileOptions::from("test");
        assert_eq!(file_options.file_extension, None);
        assert_eq!(file_options.file_compression_type, None);
    }
}
