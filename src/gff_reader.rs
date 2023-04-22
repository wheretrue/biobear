use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use std::vec;

use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;

use arrow::array::*;
use arrow::datatypes::*;
use pyo3::types::PyBytes;

struct GFFBatch {
    seqnames: GenericStringBuilder<i32>,
    sources: GenericStringBuilder<i32>,
    feature_types: GenericStringBuilder<i32>,
    starts: Int64Builder,
    ends: Int64Builder,
    scores: Float32Builder,
    strands: GenericStringBuilder<i32>,
    phases: GenericStringBuilder<i32>,
    attributes: GenericStringBuilder<i32>,

    schema: Schema,
}

impl GFFBatch {
    fn new() -> Self {
        let file_schema = Schema::new(vec![
            Field::new("seqname", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, true),
            Field::new("feature", DataType::Utf8, false),
            Field::new("start", DataType::Int64, false),
            Field::new("end", DataType::Int64, false),
            Field::new("score", DataType::Int64, true),
            Field::new("strand", DataType::Utf8, false),
            Field::new("phase", DataType::Utf8, true),
            Field::new("attributes", DataType::Utf8, true),
        ]);

        Self {
            seqnames: GenericStringBuilder::<i32>::new(),
            sources: GenericStringBuilder::<i32>::new(),
            feature_types: GenericStringBuilder::<i32>::new(),
            starts: Int64Builder::new(),
            ends: Int64Builder::new(),
            scores: Float32Builder::new(),
            strands: GenericStringBuilder::<i32>::new(),
            phases: GenericStringBuilder::<i32>::new(),
            attributes: GenericStringBuilder::<i32>::new(),
            schema: file_schema,
        }
    }

    fn add(&mut self, record: noodles::gff::Record) {
        self.seqnames.append_value(record.reference_sequence_name());
        self.sources.append_value(record.source());
        self.feature_types.append_value(record.ty());
        self.starts.append_value(record.start().get() as i64);
        self.ends.append_value(record.end().get() as i64);
        self.scores.append_option(record.score());
        self.strands.append_value(record.strand().to_string());
        self.phases
            .append_option(record.phase().map(|p| p.to_string()));

        let attrs = record.attributes();
        if attrs.is_empty() {
            self.attributes.append_null();
        } else {
            let mut attr_str = String::new();
            for entry in attrs.into_iter() {
                attr_str.push_str(&format!("{}={};", entry.key(), entry.value()));
            }
            self.attributes.append_value(&attr_str);
        }
    }

    fn to_batch(&mut self) -> RecordBatch {
        let seqnames = self.seqnames.finish();
        let sources = self.sources.finish();
        let feature_types = self.feature_types.finish();
        let starts = self.starts.finish();
        let ends = self.ends.finish();
        let scores = self.scores.finish();
        let strands = self.strands.finish();
        let phases = self.phases.finish();
        let attributes = self.attributes.finish();

        RecordBatch::try_new(
            Arc::new(self.schema.clone()),
            vec![
                Arc::new(seqnames),
                Arc::new(sources),
                Arc::new(feature_types),
                Arc::new(starts),
                Arc::new(ends),
                Arc::new(scores),
                Arc::new(strands),
                Arc::new(phases),
                Arc::new(attributes),
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

#[pyclass(name = "_GFFReader")]
pub struct GFFReader {
    reader: noodles::gff::Reader<BufReader<File>>,
}

#[pymethods]
impl GFFReader {
    #[new]
    fn new(path: &str) -> Self {
        let file = File::open(path).unwrap();
        let reader = noodles::gff::Reader::new(BufReader::new(file));

        Self { reader }
    }

    fn read(&mut self) -> PyObject {
        let mut batch = GFFBatch::new();
        for record in self.reader.records() {
            let record = record.unwrap();
            batch.add(record);
        }

        let ipc = batch.to_ipc();
        Python::with_gil(|py| {
            let pybytes = PyBytes::new(py, &ipc);
            pybytes.into()
        })
    }
}
