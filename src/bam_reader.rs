use pyo3::prelude::*;
use pyo3::types::PyBytes;

use noodles::core::Position;
use noodles::core::Region;

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use noodles::{bam, bgzf, sam};

use arrow::array::*;
use arrow::datatypes::*;

struct BamBatch {
    names: GenericStringBuilder<i32>,
    flags: Int32Builder,
    references: GenericStringBuilder<i32>,
    starts: Int32Builder,
    ends: Int32Builder,
    mapping_qualities: GenericStringBuilder<i32>,
    cigar: GenericStringBuilder<i32>,
    mate_references: GenericStringBuilder<i32>,
    sequences: GenericStringBuilder<i32>,
    quality_scores: GenericStringBuilder<i32>,

    schema: Schema,
}

impl BamBatch {
    fn new() -> Self {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("flag", DataType::Int32, false),
            Field::new("reference", DataType::Utf8, true),
            Field::new("start", DataType::Int32, true),
            Field::new("end", DataType::Int32, true),
            Field::new("mapping_quality", DataType::Utf8, true),
            Field::new("cigar", DataType::Utf8, false),
            Field::new("mate_reference", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality_score", DataType::Utf8, false),
        ]);

        Self {
            names: GenericStringBuilder::<i32>::new(),
            flags: Int32Builder::new(),
            references: GenericStringBuilder::<i32>::new(),
            starts: Int32Builder::new(),
            ends: Int32Builder::new(),
            mapping_qualities: GenericStringBuilder::<i32>::new(),
            cigar: GenericStringBuilder::<i32>::new(),
            mate_references: GenericStringBuilder::<i32>::new(),
            sequences: GenericStringBuilder::<i32>::new(),
            quality_scores: GenericStringBuilder::<i32>::new(),
            schema,
        }
    }

    fn add(&mut self, record: sam::alignment::Record, header: &sam::Header) {
        self.names.append_option(record.read_name());

        let flag_bits = record.flags().bits();
        self.flags.append_value(flag_bits as i32);

        let reference_name = match record.reference_sequence(header) {
            Some(Ok((name, _))) => Some(name.as_str()),
            Some(Err(_)) => None,
            None => None,
        };
        self.references.append_option(reference_name);

        self.starts
            .append_option(record.alignment_start().map(|v| v.get() as i32));

        self.ends
            .append_option(record.alignment_end().map(|v| v.get() as i32));

        self.mapping_qualities
            .append_option(record.mapping_quality().map(|v| v.get().to_string()));

        let cigar_string = record.cigar().to_string();
        self.cigar.append_value(cigar_string.as_str());

        let mate_reference_name = match record.mate_reference_sequence(header) {
            Some(Ok((name, _))) => Some(name.as_str()),
            Some(Err(_)) => None,
            None => None,
        };
        self.mate_references.append_option(mate_reference_name);

        let sequence_string = record.sequence().to_string();
        self.sequences.append_value(sequence_string.as_str());

        let quality_scores = record.quality_scores();
        self.quality_scores
            .append_value(quality_scores.to_string().as_str());
    }

    fn to_batch(&mut self) -> RecordBatch {
        let names = self.names.finish();
        let flags = self.flags.finish();
        let references = self.references.finish();
        let starts = self.starts.finish();
        let ends = self.ends.finish();
        let mapping_qualities = self.mapping_qualities.finish();
        let cigar = self.cigar.finish();
        let mate_references = self.mate_references.finish();
        let sequences = self.sequences.finish();
        let quality_scores = self.quality_scores.finish();

        RecordBatch::try_new(
            Arc::new(self.schema.clone()),
            vec![
                Arc::new(names),
                Arc::new(flags),
                Arc::new(references),
                Arc::new(starts),
                Arc::new(ends),
                Arc::new(mapping_qualities),
                Arc::new(cigar),
                Arc::new(mate_references),
                Arc::new(sequences),
                Arc::new(quality_scores),
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

#[pyclass(name = "_BamReader")]
pub struct BamReader {
    reader: bam::Reader<bgzf::Reader<BufReader<File>>>,
    header: sam::Header,
}

#[pymethods]
impl BamReader {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let file = File::open(path)?;
        let buf_reader = BufReader::new(file);
        let mut reader = bam::Reader::new(buf_reader);
        let header = reader.read_header().unwrap();

        Ok(Self { reader, header })
    }

    fn read(&mut self) -> PyResult<PyObject> {
        let mut batch = BamBatch::new();

        for record in self.reader.records(&self.header) {
            let record = record?;
            batch.add(record, &self.header);
        }

        let ipc = batch.to_ipc();
        Ok(Python::with_gil(|py| PyBytes::new(py, &ipc).into()))
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    pub fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) {}
}

#[pyclass(name = "_BamIndexedReader")]
pub struct BamIndexedReader {
    reader: bam::IndexedReader<bgzf::Reader<BufReader<File>>>,
    header: sam::Header,
}

#[pymethods]
impl BamIndexedReader {
    #[new]
    fn new(path: &str, index_path: Option<&str>) -> PyResult<Self> {
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

        Ok(Self { reader, header })
    }

    fn read(&mut self) -> PyResult<PyObject> {
        let mut batch = BamBatch::new();

        for record in self.reader.records(&self.header) {
            let record = record?;
            batch.add(record, &self.header);
        }

        let ipc = batch.to_ipc();
        Ok(Python::with_gil(|py| PyBytes::new(py, &ipc).into()))
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

        let ipc = batch.to_ipc();

        Ok(Python::with_gil(|py| PyBytes::new(py, &ipc).into()))
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    pub fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) {}
}
