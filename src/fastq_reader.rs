use pyo3::prelude::*;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use pyo3::types::PyBytes;

use std::io::BufReader;
use std::sync::Arc;

use noodles::fastq::Reader;

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

    fn to_ipc(&mut self) -> Vec<u8> {
        let batch = self.to_batch();

        let mut ipc = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut ipc, &self.schema).unwrap();
            writer.write(&batch).unwrap();

            writer.finish().unwrap();
        }
        ipc
    }
}

#[pyclass(name = "_FastqReader")]
pub struct FastqReader {
    reader: Reader<BufReader<std::fs::File>>,
}

#[pymethods]
impl FastqReader {
    #[new]
    fn new(path: &str) -> Self {
        let file = std::fs::File::open(path).unwrap();
        let reader = Reader::new(BufReader::new(file));

        Self { reader }
    }

    pub fn read(&mut self) -> PyObject {
        let mut batch = FastqBatch::new();

        for record in self.reader.records() {
            let record = record.unwrap();
            batch.add(record);
        }

        let ipc = batch.to_ipc();
        Python::with_gil(|py| PyBytes::new(py, &ipc).into())
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    pub fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) {}
}
