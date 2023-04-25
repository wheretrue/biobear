use pyo3::prelude::*;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use pyo3::types::PyBytes;

use std::io::BufReader;
use std::sync::Arc;

use noodles::fastq::Reader;

use crate::batch::BearRecordBatch;

struct FastqBatch {
    names: GenericStringBuilder<i32>,
    descriptions: GenericStringBuilder<i32>,
    sequences: GenericStringBuilder<i32>,
    qualities: GenericStringBuilder<i32>,

    schema: Schema,
}

impl FastqBatch {
    fn new() -> Self {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality", DataType::Utf8, false),
        ]);

        Self {
            names: GenericStringBuilder::<i32>::new(),
            descriptions: GenericStringBuilder::<i32>::new(),
            sequences: GenericStringBuilder::<i32>::new(),
            qualities: GenericStringBuilder::<i32>::new(),
            schema,
        }
    }

    fn add(&mut self, record: noodles::fastq::Record) {
        let name = std::str::from_utf8(record.name()).unwrap();
        self.names.append_value(name);

        let desc = record.description();
        if desc.is_empty() {
            self.descriptions.append_null();
        } else {
            let desc_str = std::str::from_utf8(desc).unwrap();
            self.descriptions.append_value(desc_str);
        }

        let record_sequence = record.sequence().as_ref();
        let sequence = std::str::from_utf8(record_sequence).unwrap();
        self.sequences.append_value(sequence);

        let record_quality = record.quality_scores().as_ref();
        let quality = std::str::from_utf8(record_quality).unwrap();
        self.qualities.append_value(quality);
    }
}

impl BearRecordBatch for FastqBatch {
    fn to_batch(&mut self) -> RecordBatch {
        let names = self.names.finish();
        let descriptions = self.descriptions.finish();
        let sequences = self.sequences.finish();
        let qualities = self.qualities.finish();

        RecordBatch::try_new(
            Arc::new(self.schema.clone()),
            vec![
                Arc::new(names),
                Arc::new(descriptions),
                Arc::new(sequences),
                Arc::new(qualities),
            ],
        )
        .unwrap()
    }
}

#[pyclass(name = "_FastqReader")]
pub struct FastqReader {
    reader: Reader<BufReader<std::fs::File>>,
}

#[pymethods]
impl FastqReader {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let file = std::fs::File::open(path)?;
        let reader = Reader::new(BufReader::new(file));

        Ok(Self { reader })
    }

    pub fn read(&mut self) -> PyResult<PyObject> {
        let mut batch = FastqBatch::new();

        for record in self.reader.records() {
            let record = record?;
            batch.add(record);
        }

        Ok(Python::with_gil(|py| {
            PyBytes::new(py, &batch.serialize()).into()
        }))
    }
}

#[pyclass(name = "_FastqGzippedReader")]
pub struct FastqGzippedReader {
    reader: Reader<BufReader<flate2::read::GzDecoder<std::fs::File>>>,
}

#[pymethods]
impl FastqGzippedReader {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let file = std::fs::File::open(path)?;
        let reader = Reader::new(BufReader::new(flate2::read::GzDecoder::new(file)));

        Ok(Self { reader })
    }

    pub fn read(&mut self) -> PyResult<PyObject> {
        let mut batch = FastqBatch::new();

        for record in self.reader.records() {
            let record = record?;
            batch.add(record);
        }

        Ok(Python::with_gil(|py| {
            PyBytes::new(py, &batch.serialize()).into()
        }))
    }
}
