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
use pyo3::{
    pyclass, pymethods,
    types::{PyAnyMethods, PyTuple},
    IntoPy, PyObject, PyResult, Python, ToPyObject,
};
use tokio::runtime::Runtime;

use crate::{
    error::{self, BioBearError},
    runtime::wait_for_future,
};

#[pyclass(name = "ExecutionResult", subclass)]
#[derive(Clone)]
pub(crate) struct ExecutionResult {
    pub(super) df: Arc<DataFrame>,
}

impl ExecutionResult {
    pub(crate) fn new(df: DataFrame) -> Self {
        Self { df: Arc::new(df) }
    }
}

#[pymethods]
impl ExecutionResult {
    /// Collect the batches and return a list of pyarrow RecordBatch
    fn collect(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let batches = wait_for_future(py, self.df.as_ref().clone().collect())
            .map_err(error::BioBearError::from)?;
        batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect()
    }

    /// Returns the schema from the logical plan
    ///
    /// Note: This is a logical schema and may not match the physical schema
    fn schema(&self) -> PyArrowType<Schema> {
        PyArrowType(self.df.schema().into())
    }

    /// Convert to an Arrow Table
    fn to_arrow_table(&self, py: Python) -> PyResult<PyObject> {
        let batches = self.collect(py)?.to_object(py);

        Python::with_gil(|py| {
            // Instantiate pyarrow Table object and use its from_batches method
            let table_class = py.import_bound("pyarrow")?.getattr("Table")?;

            let args = PyTuple::new_bound(py, &[batches]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();
            Ok(table)
        })
    }

    #[allow(clippy::wrong_self_convention)]
    /// Convert to an Arrow RecordBatchReader
    fn to_arrow_record_batch_reader(&mut self, py: Python) -> PyResult<PyObject> {
        let stream = wait_for_future(py, self.df.as_ref().clone().execute_stream())
            .map_err(error::BioBearError::from)?;

        let runtime = Arc::new(Runtime::new()?);

        let dataframe_record_batch_stream = DataFrameRecordBatchStream::new(stream, runtime);

        let mut stream = FFI_ArrowArrayStream::new(Box::new(dataframe_record_batch_stream));

        let stream_reader =
            unsafe { ArrowArrayStreamReader::from_raw(&mut stream).map_err(BioBearError::from) }?;

        stream_reader.into_pyarrow(py)
    }

    /// Convert to Arrow Table
    fn to_arrow(&self, py: Python) -> PyResult<PyObject> {
        let batches = self.collect(py)?.to_object(py);

        let schema = None::<PyArrowType<Schema>>.into_py(py);

        // Instantiate pyarrow Table object and use its from_batches method
        let table_class = py.import_bound("pyarrow")?.getattr("Table")?;

        let args = PyTuple::new_bound(py, &[batches, schema]);
        let table: PyObject = table_class.call_method1("from_batches", args)?.into();
        Ok(table)
    }

    /// Convert to a Polars DataFrame
    fn to_polars(&self, py: Python) -> PyResult<PyObject> {
        let stream = wait_for_future(py, self.df.as_ref().clone().execute_stream())
            .map_err(error::BioBearError::from)?;

        let schema = stream.schema().to_pyarrow(py)?;

        let runtime = Arc::new(Runtime::new()?);

        let dataframe_record_batch_stream = DataFrameRecordBatchStream::new(stream, runtime);

        let mut stream = FFI_ArrowArrayStream::new(Box::new(dataframe_record_batch_stream));

        let batches =
            unsafe { ArrowArrayStreamReader::from_raw(&mut stream).map_err(BioBearError::from) }?;

        let batches = batches.into_pyarrow(py)?;

        let table_class = py.import_bound("pyarrow")?.getattr("Table")?;
        let args = (batches, schema);

        let table: PyObject = table_class.call_method1("from_batches", args)?.into();

        let module = py.import_bound("polars")?;
        let args = (table,);
        let result = module.call_method1("from_arrow", args)?.into();

        Ok(result)
    }
}
