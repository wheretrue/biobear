mod fasta_batch;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use pyo3::prelude::*;

use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

use std::io;
use std::io::BufReader;

use noodles::fasta::Reader;

use crate::to_arrow::to_pyarrow;

use self::fasta_batch::add_next_record_to_batch;
use self::fasta_batch::FastaSchemaTrait;

#[pyclass(name = "_FastaReader")]
pub struct FastaReader {
    reader: Reader<BufReader<std::fs::File>>,
    file_path: String,
    batch_size: usize,
}

impl FastaSchemaTrait for FastaReader {}

impl Iterator for FastaReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        add_next_record_to_batch(&mut self.reader, self.batch_size)
    }
}

impl RecordBatchReader for FastaReader {
    fn schema(&self) -> SchemaRef {
        self.fasta_schema().into()
    }
}

impl FastaReader {
    pub fn open(path: &str, batch_size: Option<usize>) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let buf_reader = BufReader::new(file);

        let reader = Reader::new(buf_reader);

        Ok(Self {
            reader,
            file_path: path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
    }
}

#[pymethods]
impl FastaReader {
    #[new]
    fn new(fasta_path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        Self::open(fasta_path, batch_size).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Error opening fasta file: {}", e))
        })
    }

    pub fn to_pyarrow(&mut self) -> PyResult<PyObject> {
        to_pyarrow(self.clone())
    }
}

impl Clone for FastaReader {
    fn clone(&self) -> Self {
        Self::open(&self.file_path, Some(self.batch_size)).unwrap()
    }
}

#[pyclass(name = "_FastaGzippedReader")]
pub struct FastaGzippedReader {
    reader: Reader<BufReader<flate2::read::GzDecoder<std::fs::File>>>,
    batch_size: usize,
    file_path: String,
}

impl FastaGzippedReader {
    pub fn open(path: &str, batch_size: Option<usize>) -> io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let gz_decoder = flate2::read::GzDecoder::new(file);
        let buf_reader = BufReader::new(gz_decoder);

        let reader = Reader::new(buf_reader);

        Ok(Self {
            reader,
            file_path: path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
    }
}

impl FastaSchemaTrait for FastaGzippedReader {}

impl Clone for FastaGzippedReader {
    fn clone(&self) -> Self {
        let file = std::fs::File::open(&self.file_path).unwrap();
        let gz_decoder = flate2::read::GzDecoder::new(file);
        let buf_reader = BufReader::new(gz_decoder);

        Self {
            reader: Reader::new(buf_reader),
            file_path: self.file_path.clone(),
            batch_size: self.batch_size,
        }
    }
}

#[pymethods]
impl FastaGzippedReader {
    #[new]
    fn new(fasta_path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        Self::open(fasta_path, batch_size).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("Error opening fasta file: {}", e))
        })
    }

    pub fn to_pyarrow(&mut self) -> PyResult<PyObject> {
        to_pyarrow(self.clone())
    }
}

impl Iterator for FastaGzippedReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        add_next_record_to_batch(&mut self.reader, self.batch_size)
    }
}

impl RecordBatchReader for FastaGzippedReader {
    fn schema(&self) -> SchemaRef {
        self.fasta_schema().into()
    }
}
