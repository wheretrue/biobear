mod gff_batch;

use std::io::BufReader;
use std::{fs::File, sync::Arc};

use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ffi_stream::{export_reader_into_raw, ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::pyarrow::PyArrowConvert;
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use pyo3::prelude::*;

use pyo3::types::PyBytes;

use crate::batch::BearRecordBatch;

use self::gff_batch::{add_next_gff_record_to_batch, GFFBatch, GffSchemaTrait};

#[pyclass(name = "_GFFReader")]
pub struct GFFReader {
    reader: noodles::gff::Reader<BufReader<File>>,
    file_path: String,
    batch_size: usize,
}

#[pymethods]
impl GFFReader {
    #[new]
    fn new(path: &str, batch_size: Option<usize>) -> PyResult<Self> {
        let file = File::open(path)?;
        let reader = noodles::gff::Reader::new(BufReader::new(file));

        Ok(Self {
            reader,
            file_path: path.to_string(),
            batch_size: batch_size.unwrap_or(2048),
        })
    }

    fn read(&mut self) -> PyResult<PyObject> {
        let mut batch = GFFBatch::new();
        for record in self.reader.records() {
            let record = match record {
                Ok(record) => record,
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyIOError, _>(format!(
                        "Error reading record: {}",
                        e
                    )))
                }
            };
            batch.add(record);
        }

        let buffer = batch.serialize();
        Ok(Python::with_gil(|py| PyBytes::new(py, &buffer).into()))
    }
}

impl GffSchemaTrait for GFFReader {}

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

impl GFFReader {
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

impl Clone for GFFReader {
    fn clone(&self) -> Self {
        let file = File::open(&self.file_path).unwrap();
        let reader = noodles::gff::Reader::new(BufReader::new(file));

        Self {
            reader,
            file_path: self.file_path.clone(),
            batch_size: self.batch_size,
        }
    }
}

#[pyfunction]
pub fn gff_reader_to_pyarrow(reader: GFFReader) -> PyResult<PyObject> {
    reader.to_pyarrow()
}
