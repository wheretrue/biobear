mod genbank_batch;

use arrow::{
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};
use pyo3::prelude::*;

use std::fs::File;
use std::io::{self, BufReader};

use gb_io::reader;

use crate::to_arrow::to_pyarrow;

use self::genbank_batch::{add_next_genbank_record_to_batch, GenbankSchemaTrait};

#[pyclass(name = "_GenbankReader")]
pub struct GenbankReader {
    reader: reader::SeqReader<BufReader<File>>,
    batch_size: usize,
    file_path: String,
}

impl GenbankReader {
    fn open(path: &str, batch_size: Option<usize>) -> io::Result<Self> {
        let file = File::open(path)?;
        let reader = reader::SeqReader::new(BufReader::new(file));

        Ok(Self {
            reader,
            batch_size: batch_size.unwrap_or(2048),
            file_path: path.to_string(),
        })
    }
}

#[pymethods]
impl GenbankReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        Self::open(path, batch_size).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Error opening file {path}: {e}"))
        })
    }

    fn to_pyarrow(&self) -> PyResult<PyObject> {
        to_pyarrow(self.clone())
    }
}

impl GenbankSchemaTrait for GenbankReader {}

impl Iterator for GenbankReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        add_next_genbank_record_to_batch(&mut self.reader, Some(self.batch_size))
    }
}

impl RecordBatchReader for GenbankReader {
    fn schema(&self) -> SchemaRef {
        self.genbank_schema().into()
    }
}

impl Clone for GenbankReader {
    fn clone(&self) -> Self {
        Self::open(self.file_path.as_str(), Some(self.batch_size)).unwrap()
    }
}
