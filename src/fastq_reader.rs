mod fastq_batch;

use arrow::ffi_stream::{export_reader_into_raw, ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::pyarrow::PyArrowConvert;
use arrow::record_batch::RecordBatch;
use arrow::{error::ArrowError, record_batch::RecordBatchReader};
use pyo3::prelude::*;

use std::io::BufReader;
use std::sync::Arc;

use noodles::fastq::Reader;

use self::fastq_batch::{add_next_fastq_record_to_batch, FastqSchemaTrait};

#[pyclass(name = "_FastqReader")]
pub struct FastqReader {
    reader: Reader<BufReader<std::fs::File>>,
    file_path: String,
    batch_size: usize,
}

impl FastqSchemaTrait for FastqReader {}

#[pymethods]
impl FastqReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        let file = std::fs::File::open(path)?;
        let reader = Reader::new(BufReader::new(file));

        Ok(Self {
            reader,
            file_path: path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
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

impl FastqReader {
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

impl Clone for FastqReader {
    fn clone(&self) -> Self {
        let file = std::fs::File::open(&self.file_path).unwrap();
        let reader = Reader::new(BufReader::new(file));

        Self {
            reader,
            file_path: self.file_path.clone(),
            batch_size: self.batch_size,
        }
    }
}

#[pyfunction]
pub fn fastq_reader_to_pyarrow(reader: FastqReader) -> PyResult<PyObject> {
    reader.to_pyarrow()
}

#[pyclass(name = "_FastqGzippedReader")]
pub struct FastqGzippedReader {
    reader: Reader<BufReader<flate2::read::GzDecoder<std::fs::File>>>,
    file_path: String,
    batch_size: usize,
}

#[pymethods]
impl FastqGzippedReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        let file = std::fs::File::open(path)?;
        let reader = Reader::new(BufReader::new(flate2::read::GzDecoder::new(file)));

        Ok(Self {
            reader,
            file_path: path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
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

impl FastqGzippedReader {
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

impl Clone for FastqGzippedReader {
    fn clone(&self) -> Self {
        let file = std::fs::File::open(&self.file_path).unwrap();
        let reader = Reader::new(BufReader::new(flate2::read::GzDecoder::new(file)));

        Self {
            reader,
            file_path: self.file_path.clone(),
            batch_size: self.batch_size,
        }
    }
}

#[pyfunction]
pub fn fastq_gzipped_reader_to_pyarrow(reader: FastqGzippedReader) -> PyResult<PyObject> {
    reader.to_pyarrow()
}
