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

use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::pyarrow::IntoPyArrow;
use datafusion::prelude::SessionContext;
use exon::ffi::DataFrameRecordBatchStream;
use exon::new_exon_config;
use exon::ExonSessionExt;
use pyo3::prelude::*;

use tokio::runtime::Runtime;

use std::io;
use std::sync::Arc;

#[pyclass(name = "_BamIndexedReader")]
pub struct BamIndexedReader {
    path: String,
    batch_size: Option<usize>,
    _runtime: Arc<Runtime>,
}

impl BamIndexedReader {
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
impl BamIndexedReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        Self::open(path, batch_size).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "Failed to open file: {path} with error: {e}"
            ))
        })
    }

    fn query(&mut self, region: &str) -> PyResult<PyObject> {
        let mut config = new_exon_config();
        if let Some(batch_size) = self.batch_size {
            config = config.with_batch_size(batch_size);
        }

        let ctx = SessionContext::with_config_exon(config);

        let df = self._runtime.block_on(async {
            ctx.sql(&format!(
                "CREATE EXTERNAL TABLE bam_file STORED AS INDEXED_BAM LOCATION '{}'",
                self.path.as_str()
            ))
            .await?;

            let sql = format!(
                "SELECT * FROM bam_file WHERE bam_region_filter('{}', reference) = true",
                region
            );

            match ctx.sql(&sql).await {
                Ok(df) => Ok(df),
                Err(e) => Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error reading BAM file: {e}"),
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
