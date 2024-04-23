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

use noodles::core::Region;
use pyo3::{pyclass, pymethods, PyResult};

#[pyclass]
#[derive(Debug, Clone, Default)]
/// Options for reading BigWig with a zoom.
pub struct BigWigReadOptions {
    zoom: Option<u32>,
    region: Option<Region>,
}

impl BigWigReadOptions {
    pub fn zoom(&self) -> Option<u32> {
        self.zoom
    }
}

#[pymethods]
impl BigWigReadOptions {
    #[new]
    #[pyo3(signature = (/, zoom = None, region = None))]
    fn try_new(zoom: Option<u32>, region: Option<String>) -> PyResult<Self> {
        let region = region
            .map(|r| Region::from_str(&r))
            .transpose()
            .map_err(|e| {
                crate::error::BioBearError::ParserError(format!(
                    "Couldn\'t parse region error {}",
                    e
                ))
            })?;

        Ok(Self { zoom, region })
    }
}

impl TryFrom<BigWigReadOptions> for exon::datasources::bigwig::zoom::ListingTableOptions {
    type Error = crate::error::BioBearError;

    fn try_from(options: BigWigReadOptions) -> crate::error::BioBearResult<Self> {
        let zoom = if let Some(z) = options.zoom {
            z
        } else {
            return Err(crate::error::BioBearError::ParserError(
                "Zoom level is required".to_string(),
            ));
        };

        Ok(exon::datasources::bigwig::zoom::ListingTableOptions::new(
            zoom,
        ))
    }
}

impl TryFrom<BigWigReadOptions> for exon::datasources::bigwig::value::ListingTableOptions {
    type Error = crate::error::BioBearError;

    fn try_from(options: BigWigReadOptions) -> crate::error::BioBearResult<Self> {
        let region = options.region;

        let options = exon::datasources::bigwig::value::ListingTableOptions::default();

        if let Some(_region) = region {
            todo!("Set region")
            // Ok(options.with_region(region))
        } else {
            Ok(options)
        }
    }
}
