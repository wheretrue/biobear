mod vcf_batch;

use arrow::{
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use std::fs::File;
use std::io::{self, BufReader};

use noodles::vcf;

use crate::{batch::BearRecordBatch, to_arrow::to_pyarrow};

use self::vcf_batch::{
    add_next_vcf_indexed_record_to_batch, add_next_vcf_record_to_batch, VCFBatch, VCFSchemaTrait,
};

#[pyclass(name = "_VCFReader")]
pub struct VCFReader {
    reader: vcf::Reader<BufReader<File>>,
    header: vcf::Header,
    batch_size: usize,
    file_path: String,
}

impl VCFReader {
    fn open(path: &str, batch_size: Option<usize>) -> io::Result<Self> {
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

#[pymethods]
impl VCFReader {
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

impl VCFSchemaTrait for VCFReader {}

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
        Self::open(self.file_path.as_str(), Some(self.batch_size)).unwrap()
    }
}

#[pyclass(name = "_VCFIndexedReader")]
pub struct VCFIndexedReader {
    reader: vcf::IndexedReader<File>,
    header: vcf::Header,
    file_path: String,
    batch_size: usize,
}

impl VCFIndexedReader {
    fn open(path: &str, batch_size: Option<usize>) -> io::Result<Self> {
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
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        Self::open(path, batch_size).map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)
    }

    fn to_pyarrow(&self) -> PyResult<PyObject> {
        to_pyarrow(self.clone())
    }

    fn query(&mut self, region: &str) -> PyResult<PyObject> {
        let mut batch = VCFBatch::new();

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

impl VCFSchemaTrait for VCFIndexedReader {}

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
        Self::open(&self.file_path, Some(self.batch_size)).unwrap()
    }
}
