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

use std::str::FromStr;
use std::sync::Arc;

use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::pyarrow::IntoPyArrow;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use exon::datasources::ExonFileType;
use exon::ffi::DataFrameRecordBatchStream;
use exon::{new_exon_config, ExonRuntimeEnvExt, ExonSession};
use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::error::BioBearError;

#[pyclass(name = "_ExonReader")]
pub struct ExonReader {
    df: datafusion::dataframe::DataFrame,
    exhausted: bool,
    _runtime: Arc<Runtime>,
}

impl ExonReader {
    fn open(
        path: &str,
        file_type: ExonFileType,
        compression: Option<FileCompressionType>,
        batch_size: Option<usize>,
    ) -> Result<Self, BioBearError> {
        let rt = Arc::new(Runtime::new()?);

        let mut config = new_exon_config();

        if let Some(batch_size) = batch_size {
            config = config.with_batch_size(batch_size);
        }

        let ctx = ExonSession::with_config_exon(config).map_err(BioBearError::from)?;

        let df = rt.block_on(async {
            ctx.session
                .runtime_env()
                .exon_register_object_store_uri(path)
                .await
                .map_err(BioBearError::from)?;

            let df = ctx
                .read_exon_table(path, file_type, compression)
                .await
                .map_err(BioBearError::from)?;

            Ok::<_, BioBearError>(df)
        })?;

        Ok(Self {
            df,
            _runtime: rt,
            exhausted: false,
        })
    }
}

#[pymethods]
impl ExonReader {
    #[new]
    #[pyo3(signature = (path, file_type, compression=None, batch_size=None))]
    fn new(
        path: &str,
        file_type: &str,
        compression: Option<&str>,
        batch_size: Option<usize>,
    ) -> PyResult<Self> {
        let exon_file_type = ExonFileType::from_str(file_type).map_err(BioBearError::from)?;

        let file_compression_type = compression
            .map(FileCompressionType::from_str)
            .transpose()
            .map_err(BioBearError::from)?;

        let open = Self::open(path, exon_file_type, file_compression_type, batch_size)
            .map_err(BioBearError::from)?;

        Ok(open)
    }

    fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    #[allow(clippy::wrong_self_convention)]
    fn to_pyarrow(&mut self) -> PyResult<PyObject> {
        let mut stream_ptr = self._runtime.block_on(async {
            let stream = self
                .df
                .clone()
                .execute_stream()
                .await
                .map_err::<BioBearError, _>(|e| e.into())?;

            let dataset_record_batch_stream =
                DataFrameRecordBatchStream::new(stream, self._runtime.clone());

            Ok::<FFI_ArrowArrayStream, PyErr>(FFI_ArrowArrayStream::new(Box::new(
                dataset_record_batch_stream,
            )))
        })?;

        self.exhausted = true;

        Python::with_gil(|py| unsafe {
            ArrowArrayStreamReader::from_raw(&mut stream_ptr)
                .map_err(BioBearError::from)?
                .into_pyarrow(py)
        })
    }
}
