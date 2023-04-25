use arrow::error::ArrowError;
use arrow::ffi_stream::export_reader_into_raw;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::pyarrow::PyArrowConvert;
use arrow::record_batch::RecordBatchReader;
use pyo3::prelude::*;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use pyo3::types::PyBytes;

use std::io::BufReader;
use std::sync::Arc;

use noodles::fasta::Reader;

use crate::batch::BearRecordBatch;

struct FastaBatch {
    names: GenericStringBuilder<i32>,
    descriptions: GenericStringBuilder<i32>,
    sequences: GenericStringBuilder<i32>,

    batch_size: usize,

    schema: Schema,
}

impl FastaBatch {
    fn new(schema: Schema) -> Self {
        Self {
            names: GenericStringBuilder::<i32>::new(),
            descriptions: GenericStringBuilder::<i32>::new(),
            sequences: GenericStringBuilder::<i32>::new(),
            schema,
            batch_size: 0,
        }
    }

    fn add(&mut self, record: noodles::fasta::Record) {
        self.names.append_value(record.name());

        match record.description() {
            Some(description) => self.descriptions.append_value(description),
            None => self.descriptions.append_null(),
        }

        let record_sequence = record.sequence().as_ref();
        let sequence = std::str::from_utf8(record_sequence).unwrap();
        self.sequences.append_value(sequence);

        self.batch_size += 1;
    }

    fn add_from_parts(&mut self, name: &str, description: Option<&str>, sequence: &str) {
        self.names.append_value(name);

        match description {
            Some(description) => self.descriptions.append_value(description),
            None => self.descriptions.append_null(),
        }

        self.sequences.append_value(sequence);

        eprintln!(
            "Added record: {} {} {}",
            name,
            description.unwrap_or(""),
            sequence
        );

        self.batch_size += 1;
    }
}

impl BearRecordBatch for FastaBatch {
    fn to_batch(&mut self) -> RecordBatch {
        let names = self.names.finish();
        let descriptions = self.descriptions.finish();
        let sequences = self.sequences.finish();

        RecordBatch::try_new(
            Arc::new(self.schema.clone()),
            vec![Arc::new(names), Arc::new(descriptions), Arc::new(sequences)],
        )
        .unwrap()
    }
}

#[pyclass(name = "_FastaReader")]
pub struct FastaReader {
    reader: Reader<BufReader<std::fs::File>>,
    file_path: String,

    schema: Schema,
    batch_size: usize,
}

impl Iterator for FastaReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut fasta_batch = FastaBatch::new(self.schema.clone());

        for _i in 1..self.batch_size {
            let mut buf = String::new();
            let mut sequence = Vec::new();

            match self.reader.read_definition(&mut buf) {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        let ii = fasta_batch.to_batch();

                        if ii.num_rows() == 0 {
                            return None;
                        }

                        return Some(Ok(ii));
                    }
                }
                Err(e) => return Some(Err(ArrowError::ExternalError(Box::new(e)))),
            }

            match self.reader.read_sequence(&mut sequence) {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        let ii = fasta_batch.to_batch();

                        if ii.num_rows() == 0 {
                            return None;
                        }

                        return Some(Ok(ii));
                    }
                }
                Err(e) => return Some(Err(ArrowError::ExternalError(Box::new(e)))),
            }

            let sequence_str = std::str::from_utf8(&sequence).unwrap();

            match buf.strip_prefix(">") {
                None => {
                    panic!("Invalid fasta header");
                }
                Some(definition) => match definition.split_once(" ") {
                    Some((id, description)) => {
                        eprintln!("Adding {} {}", id, description);
                        fasta_batch.add_from_parts(id, Some(description), sequence_str);
                    }
                    None => fasta_batch.add_from_parts(definition, None, sequence_str),
                },
            }
        }

        Some(Ok(fasta_batch.to_batch()))
    }
}

impl RecordBatchReader for FastaReader {
    fn schema(&self) -> SchemaRef {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
        ]);

        schema.into()
    }
}

#[pymethods]
impl FastaReader {
    #[new]
    fn new(fasta_path: &str) -> PyResult<Self> {
        let file = std::fs::File::open(fasta_path)?;
        let buf_reader = BufReader::new(file);

        let reader = Reader::new(buf_reader);

        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
        ]);

        Ok(Self {
            reader,
            file_path: fasta_path.to_string(),
            schema,
            batch_size: 1000,
        })
    }

    pub fn read(&mut self) -> PyResult<PyObject> {
        let mut batch = FastaBatch::new(self.schema.clone());

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
            schema: self.schema.clone(),
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
    schema: Schema,
    batch_size: usize,
    file_path: String,
}

impl Clone for FastaGzippedReader {
    fn clone(&self) -> Self {
        let file = std::fs::File::open(&self.file_path).unwrap();
        let gz_decoder = flate2::read::GzDecoder::new(file);
        let buf_reader = BufReader::new(gz_decoder);

        Self {
            reader: Reader::new(buf_reader),
            file_path: self.file_path.clone(),
            schema: self.schema.clone(),
            batch_size: self.batch_size,
        }
    }
}

#[pymethods]
impl FastaGzippedReader {
    #[new]
    fn new(fasta_path: &str) -> PyResult<Self> {
        let file = std::fs::File::open(fasta_path)?;
        let gz_decoder = flate2::read::GzDecoder::new(file);
        let buf_reader = BufReader::new(gz_decoder);

        let reader = Reader::new(buf_reader);

        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
        ]);

        Ok(Self {
            reader,
            file_path: fasta_path.to_string(),
            schema,
            batch_size: 1000,
        })
    }

    pub fn read(&mut self) -> PyResult<PyObject> {
        let mut batch = FastaBatch::new(self.schema.clone());

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
        let mut fasta_batch = FastaBatch::new(self.schema.clone());

        for _i in 1..self.batch_size {
            let mut buf = String::new();
            let mut sequence = Vec::new();

            match self.reader.read_definition(&mut buf) {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        let ii = fasta_batch.to_batch();

                        if ii.num_rows() == 0 {
                            return None;
                        }

                        return Some(Ok(ii));
                    }
                }
                Err(e) => return Some(Err(ArrowError::ExternalError(Box::new(e)))),
            }

            match self.reader.read_sequence(&mut sequence) {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        let ii = fasta_batch.to_batch();

                        if ii.num_rows() == 0 {
                            return None;
                        }

                        return Some(Ok(ii));
                    }
                }
                Err(e) => return Some(Err(ArrowError::ExternalError(Box::new(e)))),
            }

            let sequence_str = std::str::from_utf8(&sequence).unwrap();

            match buf.strip_prefix(">") {
                None => {
                    panic!("Invalid fasta header");
                }
                Some(definition) => match definition.split_once(" ") {
                    Some((id, description)) => {
                        eprintln!("Adding {} {}", id, description);
                        fasta_batch.add_from_parts(id, Some(description), sequence_str);
                    }
                    None => fasta_batch.add_from_parts(definition, None, sequence_str),
                },
            }
        }

        Some(Ok(fasta_batch.to_batch()))
    }
}

impl RecordBatchReader for FastaGzippedReader {
    fn schema(&self) -> SchemaRef {
        Arc::new(self.schema.clone())
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
