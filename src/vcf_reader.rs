// Copyright 2023 WHERE TRUE Technologies.
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

use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::pyarrow::IntoPyArrow;
use datafusion::prelude::SessionContext;
use exon::ffi::DataFrameRecordBatchStream;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use exon::{new_exon_config, ExonSessionExt};

use std::io;
use std::sync::Arc;

#[pyclass(name = "_VCFIndexedReader")]
pub struct VCFIndexedReader {
    path: String,
    batch_size: Option<usize>,
    _runtime: Arc<Runtime>,
}

impl VCFIndexedReader {
    fn open(path: &str, batch_size: Option<usize>) -> io::Result<Self> {
        // Check the path exists
        if !std::path::Path::new(path).exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("File not found: {path}"),
            ));
        }

        let rt = Arc::new(Runtime::new().unwrap());

        Ok(Self {
            path: path.to_string(),
            batch_size,
            _runtime: rt,
        })
    }
}

#[pymethods]
impl VCFIndexedReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        Self::open(path, batch_size).map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)
    }

    fn query(&mut self, region: &str) -> PyResult<PyObject> {
        let mut config = new_exon_config();
        if let Some(batch_size) = self.batch_size {
            config = config.with_batch_size(batch_size);
        }

        let ctx = SessionContext::with_config_exon(config);

        let df = self._runtime.block_on(async {
            match ctx.query_vcf_file(self.path.as_str(), region).await {
                Ok(df) => Ok(df),
                Err(e) => Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error reading VCF file: {e}"),
                )),
            }
        })?;

        let mut stream_ptr = self._runtime.block_on(async {
            let stream = df.execute_stream().await.unwrap();
            let dataset_record_batch_stream =
                DataFrameRecordBatchStream::new(stream, self._runtime.clone());

            FFI_ArrowArrayStream::new(Box::new(dataset_record_batch_stream))
        });

        Python::with_gil(|py| unsafe {
            match ArrowArrayStreamReader::from_raw(&mut stream_ptr) {
                Ok(stream_reader) => stream_reader.into_pyarrow(py),
                Err(err) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Error converting to pyarrow: {err}"
                ))),
            }
        })
    }
}
