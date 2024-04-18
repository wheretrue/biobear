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

use std::{str::FromStr, vec};

use exon::datasources::bcf::table_provider::ListingBCFTableOptions;
use noodles::core::Region;
use pyo3::{pyclass, pymethods};

use crate::error::{BioBearError, BioBearResult};

#[pyclass]
#[derive(Default, Debug, Clone)]
pub struct BCFReadOptions {
    region: Option<Region>,
}

#[pymethods]
impl BCFReadOptions {
    #[new]
    #[pyo3(signature = (/, region = None))]
    fn try_new(region: Option<String>) -> BioBearResult<Self> {
        let region = region
            .map(|r| Region::from_str(&r))
            .transpose()
            .map_err(|e| {
                BioBearError::ParserError(format!("Couldn\'t parse region error {}", e))
            })?;

        Ok(Self { region })
    }
}

impl From<BCFReadOptions> for ListingBCFTableOptions {
    fn from(options: BCFReadOptions) -> Self {
        let region = if let Some(r) = options.region {
            vec![r]
        } else {
            vec![]
        };

        ListingBCFTableOptions::default().with_regions(region)
    }
}
