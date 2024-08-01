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

use crate::FileCompressionType;

pub(crate) trait SettableFromFileOptions {
    fn file_extension_mut(&mut self) -> &mut Option<String>;

    fn file_compression_type_mut(&mut self) -> &mut Option<FileCompressionType>;
}

macro_rules! impl_settable_from_file_options {
    ($struct_name:ident) => {
        impl crate::file_options::SettableFromFileOptions for $struct_name {
            fn file_extension_mut(&mut self) -> &mut Option<String> {
                &mut self.file_extension
            }

            fn file_compression_type_mut(&mut self) -> &mut Option<FileCompressionType> {
                &mut self.file_compression_type
            }
        }
    };
}

pub(crate) use impl_settable_from_file_options;
