mod bam_batch;

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi_stream::export_reader_into_raw;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::pyarrow::PyArrowConvert;
use arrow::record_batch::RecordBatch;
use arrow::record_batch::RecordBatchReader;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use noodles::core::Position;
use noodles::core::Region;

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use crate::batch::BearRecordBatch;

use self::bam_batch::add_next_bam_indexed_record_to_batch;
use self::bam_batch::add_next_bam_record_to_batch;
use self::bam_batch::BamBatch;
use self::bam_batch::BamSchemaTrait;

use noodles::{bam, bgzf, sam};

#[pyclass(name = "_BamReader")]
pub struct BamReader {
    reader: bam::Reader<bgzf::Reader<BufReader<File>>>,
    header: sam::Header,
    file_path: String,
    batch_size: usize,
}

#[pymethods]
impl BamReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        let file = File::open(path)?;
        let buf_reader = BufReader::new(file);
        let mut reader = bam::Reader::new(buf_reader);
        let header = reader.read_header().unwrap();

        Ok(Self {
            reader,
            header,
            file_path: path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
    }
}

impl BamSchemaTrait for BamReader {}

impl Iterator for BamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        add_next_bam_record_to_batch(&mut self.reader, &self.header, Some(self.batch_size))
    }
}

impl RecordBatchReader for BamReader {
    fn schema(&self) -> SchemaRef {
        self.bam_schema().into()
    }
}

impl BamReader {
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

impl Clone for BamReader {
    fn clone(&self) -> Self {
        let file = File::open(self.file_path.clone()).unwrap();
        let buf_reader = BufReader::new(file);
        let mut reader = bam::Reader::new(buf_reader);
        let header = reader.read_header().unwrap();

        Self {
            reader,
            header,
            file_path: self.file_path.clone(),
            batch_size: self.batch_size,
        }
    }
}

#[pyfunction]
pub fn bam_reader_to_pyarrow(reader: BamReader) -> PyResult<PyObject> {
    reader.to_pyarrow()
}

#[pyclass(name = "_BamIndexedReader")]
pub struct BamIndexedReader {
    reader: bam::IndexedReader<bgzf::Reader<BufReader<File>>>,
    file_path: String,
    header: sam::Header,
    batch_size: usize,
}

#[pymethods]
impl BamIndexedReader {
    #[new]
    fn new(path: &str, index_path: Option<&str>, batch_size: Option<usize>) -> PyResult<Self> {
        let file = File::open(path)?;

        let buf_reader = BufReader::new(file);

        let inferred_path = match index_path {
            Some(path) => path.to_string(),
            None => format!("{}.bai", path),
        };

        let index = bam::bai::read(inferred_path)?;

        let mut reader = match bam::indexed_reader::Builder::default()
            .set_index(index)
            .build_from_reader(buf_reader)
        {
            Ok(reader) => reader,
            Err(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to open file: {}",
                    path
                )))
            }
        };

        let header = match reader.read_header() {
            Ok(header) => header,
            Err(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                    "Failed to read header: {}",
                    path
                )))
            }
        };

        Ok(Self {
            reader,
            file_path: path.to_string(),
            header,
            batch_size: batch_size.unwrap_or(2048),
        })
    }

    fn query(&mut self, chromosome: &str, start: usize, end: usize) -> PyResult<PyObject> {
        let mut batch = BamBatch::new();

        let start = Position::try_from(start)?;
        let end = Position::try_from(end)?;
        let query_result = self
            .reader
            .query(&self.header, &Region::new(chromosome, start..=end));

        let query = match query_result {
            Ok(query) => query,
            Err(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Failed to query region: {}:{}-{}",
                    chromosome, start, end
                )))
            }
        };

        for record in query {
            let record = record?;
            batch.add(record, &self.header);
        }

        Ok(Python::with_gil(|py| {
            PyBytes::new(py, &batch.serialize()).into()
        }))
    }
}

impl BamSchemaTrait for BamIndexedReader {}

impl Iterator for BamIndexedReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        add_next_bam_indexed_record_to_batch(&mut self.reader, &self.header, Some(self.batch_size))
    }
}

impl RecordBatchReader for BamIndexedReader {
    fn schema(&self) -> SchemaRef {
        self.bam_schema().into()
    }
}

impl BamIndexedReader {
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

impl Clone for BamIndexedReader {
    fn clone(&self) -> Self {
        let file = File::open(self.file_path.clone()).unwrap();
        let buf_reader = BufReader::new(file);

        let index = bam::bai::read(format!("{}.bai", self.file_path)).unwrap();

        let mut reader = bam::indexed_reader::Builder::default()
            .set_index(index)
            .build_from_reader(buf_reader)
            .unwrap();

        let header = reader.read_header().unwrap();

        Self {
            reader,
            header,
            file_path: self.file_path.clone(),
            batch_size: self.batch_size,
        }
    }
}

#[pyfunction]
pub fn bam_indexed_reader_to_pyarrow(reader: BamIndexedReader) -> PyResult<PyObject> {
    reader.to_pyarrow()
}
