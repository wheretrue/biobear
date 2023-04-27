mod gff_batch;

use std::fs::File;
use std::io::{self, BufReader};

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use pyo3::prelude::*;

use crate::to_arrow::to_pyarrow;

use self::gff_batch::{add_next_gff_record_to_batch, GFFSchemaTrait};

#[pyclass(name = "_GFFReader")]
pub struct GFFReader {
    reader: noodles::gff::Reader<BufReader<File>>,
    file_path: String,
    batch_size: usize,
}

impl GFFReader {
    fn open(path: &str, batch_size: Option<usize>) -> io::Result<Self> {
        let file = File::open(path)?;
        let reader = noodles::gff::Reader::new(BufReader::new(file));

        Ok(Self {
            reader,
            file_path: path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
    }
}

#[pymethods]
impl GFFReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        Self::open(path, batch_size).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "Error opening file {}: {}",
                path, e
            ))
        })
    }

    fn to_pyarrow(&self) -> PyResult<PyObject> {
        to_pyarrow(self.clone())
    }
}

impl GFFSchemaTrait for GFFReader {}

impl Iterator for GFFReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        add_next_gff_record_to_batch(&mut self.reader, Some(self.batch_size))
    }
}

impl RecordBatchReader for GFFReader {
    fn schema(&self) -> SchemaRef {
        self.gff_schema().into()
    }
}

impl Clone for GFFReader {
    fn clone(&self) -> Self {
        Self::open(&self.file_path, Some(self.batch_size)).unwrap()
    }
}

#[pyclass(name = "_GFFGzippedReader")]
pub struct GFFGzippedReader {
    reader: noodles::gff::Reader<BufReader<flate2::read::GzDecoder<std::fs::File>>>,
    file_path: String,
    batch_size: usize,
}

impl GFFGzippedReader {
    fn open(path: &str, batch_size: Option<usize>) -> io::Result<Self> {
        let file = File::open(path)?;
        let reader = noodles::gff::Reader::new(BufReader::new(flate2::read::GzDecoder::new(file)));

        Ok(Self {
            reader,
            file_path: path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
    }
}

impl GFFSchemaTrait for GFFGzippedReader {}

impl Clone for GFFGzippedReader {
    fn clone(&self) -> Self {
        Self::open(&self.file_path, Some(self.batch_size)).unwrap()
    }
}

impl Iterator for GFFGzippedReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        add_next_gff_record_to_batch(&mut self.reader, Some(self.batch_size))
    }
}

#[pymethods]
impl GFFGzippedReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        Self::open(path, batch_size).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "Error opening file {}: {}",
                path, e
            ))
        })
    }

    fn to_pyarrow(&self) -> PyResult<PyObject> {
        to_pyarrow(self.clone())
    }
}

impl RecordBatchReader for GFFGzippedReader {
    fn schema(&self) -> SchemaRef {
        self.gff_schema().into()
    }
}
