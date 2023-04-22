use pyo3::prelude::*;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use pyo3::types::PyBytes;

use std::io::BufReader;
use std::sync::Arc;

use noodles::fasta::Reader;

struct FastaBatch {
    names: GenericStringBuilder<i32>,
    descriptions: GenericStringBuilder<i32>,
    sequences: GenericStringBuilder<i32>,

    schema: Schema,
}

impl FastaBatch {
    fn new() -> Self {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
        ]);

        Self {
            names: GenericStringBuilder::<i32>::new(),
            descriptions: GenericStringBuilder::<i32>::new(),
            sequences: GenericStringBuilder::<i32>::new(),
            schema,
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
    }

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

    fn to_ipc(&mut self) -> Vec<u8> {
        let batch = self.to_batch();

        let mut ipc = Vec::new();
        {
            let cursor = std::io::Cursor::new(&mut ipc);
            let mut writer = FileWriter::try_new(cursor, &self.schema).unwrap();

            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        ipc
    }
}

#[pyclass(name = "_FastaReader")]
pub struct FastaReader {
    reader: Reader<BufReader<std::fs::File>>,
}

#[pymethods]
impl FastaReader {
    #[new]
    fn new(fasta_path: &str) -> Self {
        let file = std::fs::File::open(fasta_path).unwrap();
        let buf_reader = BufReader::new(file);

        let reader = Reader::new(buf_reader);

        Self { reader }
    }

    pub fn read(&mut self) -> PyObject {
        let mut batch = FastaBatch::new();

        for result in self.reader.records() {
            let record = result.unwrap();
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
