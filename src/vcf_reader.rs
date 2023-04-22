use pyo3::prelude::*;
use pyo3::types::PyBytes;

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use noodles::vcf;

use arrow::array::*;
use arrow::datatypes::*;

struct VcfBatch {
    chromosomes: GenericStringBuilder<i32>,
    positions: Int32Builder,
    ids: GenericStringBuilder<i32>,
    references: GenericStringBuilder<i32>,
    alternates: GenericStringBuilder<i32>,
    qualities: Float32Builder,
    filters: GenericStringBuilder<i32>,

    //TODO: dynamic schema for info and format
    infos: GenericStringBuilder<i32>,
    formats: GenericStringBuilder<i32>,

    schema: Schema,
}

impl VcfBatch {
    fn new() -> Self {
        let schema = Schema::new(vec![
            Field::new("chromosome", DataType::Utf8, false),
            Field::new("position", DataType::Int32, false),
            Field::new("id", DataType::Utf8, true),
            Field::new("reference", DataType::Utf8, false),
            Field::new("alternate", DataType::Utf8, false),
            Field::new("quality_score", DataType::Float32, true),
            Field::new("filter", DataType::Utf8, true),
            Field::new("info", DataType::Utf8, true),
            Field::new("format", DataType::Utf8, true),
        ]);

        Self {
            chromosomes: GenericStringBuilder::<i32>::new(),
            positions: Int32Builder::new(),
            ids: GenericStringBuilder::<i32>::new(),
            references: GenericStringBuilder::<i32>::new(),
            alternates: GenericStringBuilder::<i32>::new(),
            qualities: Float32Builder::new(),
            filters: GenericStringBuilder::<i32>::new(),
            infos: GenericStringBuilder::<i32>::new(),
            formats: GenericStringBuilder::<i32>::new(),

            schema,
        }
    }

    fn add(&mut self, record: &vcf::Record) {
        let chromosome: String = format!("{}", record.chromosome());
        self.chromosomes.append_value(chromosome);

        let position: usize = record.position().into();
        self.positions.append_value(position as i32);

        let id: String = format!("{}", record.ids());
        self.ids.append_value(id);

        let reference: String = format!("{}", record.reference_bases());
        self.references.append_value(reference);

        let alternate: String = format!("{}", record.alternate_bases());
        self.alternates.append_value(alternate);

        let quality = record.quality_score().map(f32::from);
        self.qualities.append_option(quality);

        let filter = record.filters().map(|filters| format!("{}", filters));
        self.filters.append_option(filter);

        let info: String = format!("{}", record.info());
        self.infos.append_value(info);

        let format: String = format!("{}", record.format());
        self.formats.append_value(format);
    }

    fn to_batch(&mut self) -> RecordBatch {
        let chromosomes = self.chromosomes.finish();
        let positions = self.positions.finish();
        let ids = self.ids.finish();
        let references = self.references.finish();
        let alternates = self.alternates.finish();
        let qualities = self.qualities.finish();
        let filters = self.filters.finish();
        let infos = self.infos.finish();
        let formats = self.formats.finish();

        RecordBatch::try_new(
            Arc::new(self.schema.clone()),
            vec![
                Arc::new(chromosomes),
                Arc::new(positions),
                Arc::new(ids),
                Arc::new(references),
                Arc::new(alternates),
                Arc::new(qualities),
                Arc::new(filters),
                Arc::new(infos),
                Arc::new(formats),
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

#[pyclass(name = "_VCFReader")]
pub struct VCFReader {
    reader: vcf::Reader<BufReader<File>>,
    header: vcf::Header,
}

#[pymethods]
impl VCFReader {
    #[new]
    fn new(path: &str) -> Self {
        let file = File::open(path).unwrap();
        let mut reader = vcf::Reader::new(BufReader::new(file));
        let header = reader.read_header().unwrap();

        Self { reader, header }
    }

    fn read(&mut self) -> PyObject {
        let mut batch = VcfBatch::new();

        for record in self.reader.records(&self.header) {
            let record = record.unwrap();
            batch.add(&record);
        }

        let ipc = batch.to_ipc();
        Python::with_gil(|py| PyBytes::new(py, &ipc).into())
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    pub fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) {}
}

#[pyclass(name = "_VCFIndexedReader")]
pub struct VCFIndexedReader {
    reader: vcf::IndexedReader<File>,
    header: vcf::Header,
}

#[pymethods]
impl VCFIndexedReader {
    #[new]
    fn new(path: &str) -> Self {
        let mut reader = vcf::indexed_reader::Builder::default()
            .build_from_path(path)
            .unwrap();

        let header = reader.read_header().unwrap();

        Self { reader, header }
    }

    fn read(&mut self) -> PyObject {
        let mut batch = VcfBatch::new();

        for record in self.reader.records(&self.header) {
            let record = record.unwrap();
            batch.add(&record);
        }

        let ipc = batch.to_ipc();
        Python::with_gil(|py| PyBytes::new(py, &ipc).into())
    }

    fn query(&mut self, region: &str) -> PyObject {
        let mut batch = VcfBatch::new();

        let region = region.parse().unwrap();
        let mut iter = self.reader.query(&self.header, &region).unwrap();

        while let Some(record) = iter.next() {
            let record = record.unwrap();
            batch.add(&record);
        }

        let ipc = batch.to_ipc();
        Python::with_gil(|py| PyBytes::new(py, &ipc).into())
    }

    pub fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    pub fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) {}
}
