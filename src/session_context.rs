use std::sync::Arc;

use arrow::pyarrow::ToPyArrow;
use datafusion::prelude::{DataFrame, SessionContext};
use exon::ExonSessionExt;

use pyo3::{prelude::*, types::PyTuple};

use crate::runtime::wait_for_future;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::PyArrowType;

#[pyclass]
pub struct ExonSessionContext {
    ctx: SessionContext,
}

impl Default for ExonSessionContext {
    fn default() -> Self {
        Self {
            ctx: SessionContext::new_exon(),
        }
    }
}

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
        let batches = wait_for_future(py, self.df.as_ref().clone().collect()).unwrap();
        batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect()
    }

    /// Returns the schema from the logical plan
    fn schema(&self) -> PyArrowType<Schema> {
        PyArrowType(self.df.schema().into())
    }

    /// Convert to Arrow Table
    /// Collect the batches and pass to Arrow Table
    fn to_arrow_table(&self, py: Python) -> PyResult<PyObject> {
        let batches = self.collect(py)?.to_object(py);
        let schema: PyObject = self.schema().into_py(py);

        Python::with_gil(|py| {
            // Instantiate pyarrow Table object and use its from_batches method
            let table_class = py.import("pyarrow")?.getattr("Table")?;
            let args = PyTuple::new(py, &[batches, schema]);
            let table: PyObject = table_class.call_method1("from_batches", args)?.into();
            Ok(table)
        })
    }
}

#[pymethods]
impl ExonSessionContext {
    #[new]
    fn new() -> PyResult<Self> {
        Ok(Self::default())
    }

    fn sql(&mut self, query: &str, py: Python) -> PyResult<PyExecutionResult> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result).unwrap();

        Ok(PyExecutionResult::new(df))
    }
}

#[pyfunction]
pub fn connect() -> PyResult<ExonSessionContext> {
    Ok(ExonSessionContext::default())
}
