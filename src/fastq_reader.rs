mod fastq_batch;

use arrow::record_batch::RecordBatch;
use arrow::{error::ArrowError, record_batch::RecordBatchReader};
use pyo3::prelude::*;

use std::io::{self, BufReader};

use noodles::fastq::Reader;

use crate::to_arrow::to_pyarrow;

use self::fastq_batch::{add_next_fastq_record_to_batch, FastqSchemaTrait};

#[pyclass(name = "_FastqReader")]
pub struct FastqReader {
    reader: Reader<BufReader<std::fs::File>>,
    file_path: String,
    batch_size: usize,
}

impl FastqSchemaTrait for FastqReader {}

impl FastqReader {
    pub fn open(path: &str, batch_size: Option<usize>) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let reader = Reader::new(BufReader::new(file));

        Ok(Self {
            reader,
            file_path: path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
    }
}

#[pymethods]
impl FastqReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        Self::open(path, batch_size).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "Error opening file {}: {}",
                path, e
            ))
        })
    }

    pub fn to_pyarrow(&self) -> PyResult<PyObject> {
        to_pyarrow(self.clone())
    }
}

impl Iterator for FastqReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        add_next_fastq_record_to_batch(&mut self.reader, self.batch_size)
    }
}

impl RecordBatchReader for FastqReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.fastq_schema().into()
    }
}

impl Clone for FastqReader {
    fn clone(&self) -> Self {
        Self::open(&self.file_path, Some(self.batch_size)).unwrap()
    }
}

#[pyclass(name = "_FastqGzippedReader")]
pub struct FastqGzippedReader {
    reader: Reader<BufReader<flate2::read::GzDecoder<std::fs::File>>>,
    file_path: String,
    batch_size: usize,
}

impl FastqGzippedReader {
    pub fn open(path: &str, batch_size: Option<usize>) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let reader = Reader::new(BufReader::new(flate2::read::GzDecoder::new(file)));

        Ok(Self {
            reader,
            file_path: path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
    }
}

impl Clone for FastqGzippedReader {
    fn clone(&self) -> Self {
        Self::open(self.file_path.as_str(), Some(self.batch_size)).unwrap()
    }
}

#[pymethods]
impl FastqGzippedReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        Self::open(path, batch_size).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                "Error opening file {}: {}",
                path, e
            ))
        })
    }

    pub fn to_pyarrow(&mut self) -> PyResult<PyObject> {
        to_pyarrow(self.clone())
    }
}

impl FastqSchemaTrait for FastqGzippedReader {}

impl Iterator for FastqGzippedReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        add_next_fastq_record_to_batch(&mut self.reader, self.batch_size)
    }
}

impl RecordBatchReader for FastqGzippedReader {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.fastq_schema().into()
    }
}
