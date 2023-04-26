mod vcf_batch;

use arrow::{
    datatypes::SchemaRef,
    error::ArrowError,
    ffi_stream::{export_reader_into_raw, ArrowArrayStreamReader, FFI_ArrowArrayStream},
    pyarrow::PyArrowConvert,
    record_batch::{RecordBatch, RecordBatchReader},
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use std::io::{self, BufReader};
use std::{fs::File, sync::Arc};

use noodles::vcf;

use crate::batch::BearRecordBatch;

use self::vcf_batch::{
    add_next_vcf_indexed_record_to_batch, add_next_vcf_record_to_batch, VcfBatch, VcfSchemaTrait,
};

#[pyclass(name = "_VCFReader")]
pub struct VCFReader {
    reader: vcf::Reader<BufReader<File>>,
    header: vcf::Header,
    batch_size: usize,
    file_path: String,
}

#[pymethods]
impl VCFReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        let file = File::open(path)?;
        let mut reader = vcf::Reader::new(BufReader::new(file));
        let header = reader.read_header()?;

        Ok(Self {
            reader,
            header,
            batch_size: batch_size.unwrap_or(2048),
            file_path: path.to_string(),
        })
    }
}

impl VcfSchemaTrait for VCFReader {}

impl Iterator for VCFReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        add_next_vcf_record_to_batch(&mut self.reader, &self.header, Some(self.batch_size))
    }
}

impl RecordBatchReader for VCFReader {
    fn schema(&self) -> SchemaRef {
        self.vcf_schema().into()
    }
}

impl Clone for VCFReader {
    fn clone(&self) -> Self {
        let file = File::open(self.file_path.clone()).unwrap();
        let buf_reader = BufReader::new(file);

        let mut reader = vcf::Reader::new(buf_reader);
        let header = reader.read_header().unwrap();

        Self {
            reader,
            header,
            file_path: self.file_path.clone(),
            batch_size: self.batch_size,
        }
    }
}

impl VCFReader {
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

#[pyfunction]
pub fn vcf_reader_to_pyarrow(reader: VCFReader) -> PyResult<PyObject> {
    reader.to_pyarrow()
}

#[pyclass(name = "_VCFIndexedReader")]
pub struct VCFIndexedReader {
    reader: vcf::IndexedReader<File>,
    header: vcf::Header,
    file_path: String,
    batch_size: usize,
}

impl VCFIndexedReader {
    fn new(path: &str, batch_size: Option<usize>) -> io::Result<Self> {
        let mut reader = vcf::indexed_reader::Builder::default().build_from_path(path)?;

        let header = match reader.read_header() {
            Ok(header) => header,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Error reading VCF header: {}", e),
                ))
            }
        };

        Ok(Self {
            reader,
            header,
            batch_size: batch_size.unwrap_or(2048),
            file_path: path.to_string(),
        })
    }
}

#[pymethods]
impl VCFIndexedReader {
    #[new]
    fn py_new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        Self::new(path, batch_size).map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)
    }

    fn query(&mut self, region: &str) -> PyResult<PyObject> {
        let mut batch = VcfBatch::new();

        let region = match region.parse() {
            Ok(region) => region,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Error parsing region: {}",
                    e
                )))
            }
        };

        let mut iter = match self.reader.query(&self.header, &region) {
            Ok(iter) => iter,
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Error querying VCF file: {}",
                    e
                )))
            }
        };

        while let Some(record) = iter.next() {
            let record = match record {
                Ok(record) => record,
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Error reading VCF record: {}",
                        e
                    )))
                }
            };
            batch.add(&record);
        }

        Ok(Python::with_gil(|py| {
            PyBytes::new(py, &batch.serialize()).into()
        }))
    }
}

impl VcfSchemaTrait for VCFIndexedReader {}

impl Iterator for VCFIndexedReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        add_next_vcf_indexed_record_to_batch(&mut self.reader, &self.header, Some(self.batch_size))
    }
}

impl RecordBatchReader for VCFIndexedReader {
    fn schema(&self) -> SchemaRef {
        self.vcf_schema().into()
    }
}

impl Clone for VCFIndexedReader {
    fn clone(&self) -> Self {
        let obj = Self::new(&self.file_path, Some(self.batch_size)).unwrap();
        obj
    }
}

impl VCFIndexedReader {
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

#[pyfunction]
pub fn vcf_indexed_reader_to_pyarrow(reader: VCFIndexedReader) -> PyResult<PyObject> {
    reader.to_pyarrow()
}
