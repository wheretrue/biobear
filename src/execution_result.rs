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

use std::sync::Arc;

use arrow::{
    datatypes::Schema,
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
    pyarrow::{IntoPyArrow, PyArrowType, ToPyArrow},
};
use datafusion::prelude::DataFrame;
use exon::ffi::DataFrameRecordBatchStream;
use pyo3::{pyclass, pymethods, types::PyTuple, PyErr, PyObject, PyResult, Python, ToPyObject};
use tokio::runtime::Runtime;

use crate::{error, runtime::wait_for_future};

#[pyclass(name = "ExecutionResult", subclass)]
#[derive(Clone)]
pub(crate) struct PyExecutionResult {
    df: Arc<DataFrame>,
}

impl PyExecutionResult {
    pub(crate) fn new(df: DataFrame) -> Self {
        Self { df: Arc::new(df) }
    }
}

#[pymethods]
impl PyExecutionResult {
    /// Collect the batches and return a list of pyarrow RecordBatch
    fn collect(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let batches = wait_for_future(py, self.df.as_ref().clone().collect())
            .map_err(error::BioBearError::from)?;
        batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect()
    }

    /// Returns the schema from the logical plan
    fn schema(&self) -> PyArrowType<Schema> {
        PyArrowType(self.df.schema().into())
    }

    /// Convert to an Arrow Table
    fn to_arrow_table(&self, py: Python) -> PyResult<PyObject> {
        let batches = self.collect(py)?.to_object(py);

        Python::with_gil(|py| {
            // Instantiate pyarrow Table object and use its from_batches method
            let table_class = py.import("pyarrow")?.getattr("Table")?;

            let args = PyTuple::new(py, &[batches]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();
            Ok(table)
        })
    }

    #[allow(clippy::wrong_self_convention)]
    /// Convert to an Arrow RecordBatchReader
    fn to_arrow_record_batch_reader(&mut self, py: Python) -> PyResult<PyObject> {
        let stream = wait_for_future(py, self.df.as_ref().clone().execute_stream())
            .map_err(error::BioBearError::from)?;

        let runtime = Arc::new(Runtime::new().unwrap());

        let dataframe_record_batch_stream = DataFrameRecordBatchStream::new(stream, runtime);

        let mut stream = FFI_ArrowArrayStream::new(Box::new(dataframe_record_batch_stream));

        Python::with_gil(|py| unsafe {
            match ArrowArrayStreamReader::from_raw(&mut stream) {
                Ok(stream_reader) => stream_reader.into_pyarrow(py),
                Err(err) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Error converting to pyarrow: {err}"
                ))),
            }
        })
    }

    /// Convert to Arrow Table
    fn to_arrow(&self, py: Python) -> PyResult<PyObject> {
        let batches = self.collect(py)?.to_object(py);

        Python::with_gil(|py| {
            // Instantiate pyarrow Table object and use its from_batches method
            let table_class = py.import("pyarrow")?.getattr("Table")?;

            let args = PyTuple::new(py, &[batches]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();
            Ok(table)
        })
    }

    /// Convert to a Polars DataFrame
    fn to_polars(&self, py: Python) -> PyResult<PyObject> {
        let batches = self.collect(py)?.to_object(py);

        Python::with_gil(|py| {
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[batches]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();

            let table_class = py.import("polars")?.getattr("DataFrame")?;
            let args = PyTuple::new(py, &[table]);
            let result = table_class.call1(args)?.into();
            Ok(result)
        })
    }
}
