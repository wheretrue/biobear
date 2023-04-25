mod fasta_batch;

use arrow::error::ArrowError;
use arrow::ffi_stream::export_reader_into_raw;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::pyarrow::PyArrowConvert;
use arrow::record_batch::RecordBatchReader;
use pyo3::prelude::*;

use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use pyo3::types::PyBytes;

use std::io::BufReader;
use std::sync::Arc;

use noodles::fasta::Reader;

use crate::batch::BearRecordBatch;

use self::fasta_batch::add_next_record_to_batch;
use self::fasta_batch::FastaBatch;
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

#[pymethods]
impl FastaReader {
    #[new]
    fn new(fasta_path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        let file = std::fs::File::open(fasta_path)?;
        let buf_reader = BufReader::new(file);

        let reader = Reader::new(buf_reader);

        Ok(Self {
            reader,
            file_path: fasta_path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
    }

    pub fn read(&mut self) -> PyResult<PyObject> {
        let mut batch = FastaBatch::new();

        for result in self.reader.records() {
            let record = result?;
            batch.add(record);
        }

        let buffer = batch.serialize();
        Ok(Python::with_gil(|py| PyBytes::new(py, &buffer).into()))
    }
}

impl FastaReader {
    pub fn to_pyarrow(self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let stream = Arc::new(FFI_ArrowArrayStream::empty());
            let stream_ptr = Arc::into_raw(stream) as *mut FFI_ArrowArrayStream;

            unsafe {
                export_reader_into_raw(Box::new(self), stream_ptr);

                match ArrowArrayStreamReader::from_raw(stream_ptr) {
                    Ok(stream_reader) => stream_reader.to_pyarrow(py),
                    Err(err) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Error converting to pyarrow: {}",
                        err
                    ))),
                }
            }
        })
    }
}

impl Clone for FastaReader {
    fn clone(&self) -> Self {
        let file = std::fs::File::open(&self.file_path).unwrap();
        let buf_reader = BufReader::new(file);

        Self {
            reader: Reader::new(buf_reader),
            file_path: self.file_path.clone(),
            batch_size: self.batch_size,
        }
    }
}

#[pyfunction]
pub fn fasta_reader_to_py_arrow(reader: FastaReader) -> PyResult<PyObject> {
    reader.to_pyarrow()
}

#[pyclass(name = "_FastaGzippedReader")]
pub struct FastaGzippedReader {
    reader: Reader<BufReader<flate2::read::GzDecoder<std::fs::File>>>,
    batch_size: usize,
    file_path: String,
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
        let file = std::fs::File::open(fasta_path)?;
        let gz_decoder = flate2::read::GzDecoder::new(file);
        let buf_reader = BufReader::new(gz_decoder);

        let reader = Reader::new(buf_reader);

        Ok(Self {
            reader,
            file_path: fasta_path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
    }

    pub fn read(&mut self) -> PyResult<PyObject> {
        let mut batch = FastaBatch::new();

        for result in self.reader.records() {
            let record = result?;
            batch.add(record);
        }

        let buffer = batch.serialize();
        Ok(Python::with_gil(|py| PyBytes::new(py, &buffer).into()))
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

#[pyfunction]
pub fn fasta_gzipped_reader_to_py_arrow(reader: FastaGzippedReader) -> PyResult<PyObject> {
    reader.to_pyarrow()
}

impl FastaGzippedReader {
    pub fn to_pyarrow(self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let stream = Arc::new(FFI_ArrowArrayStream::empty());
            let stream_ptr = Arc::into_raw(stream) as *mut FFI_ArrowArrayStream;

            unsafe {
                export_reader_into_raw(Box::new(self), stream_ptr);

                match ArrowArrayStreamReader::from_raw(stream_ptr) {
                    Ok(stream_reader) => stream_reader.to_pyarrow(py),
                    Err(err) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Error converting to pyarrow: {}",
                        err
                    ))),
                }
            }
        })
    }
}
