use std::{
    io::{BufRead, Read},
    sync::Arc,
};

use arrow::{
    array::{Float32Builder, GenericStringBuilder, Int32Builder},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use noodles::vcf;

use crate::batch::BearRecordBatch;

pub trait VCFSchemaTrait {
    fn vcf_schema(&self) -> Schema {
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

        schema
    }
}

pub struct VCFBatch {
    chromosomes: GenericStringBuilder<i32>,
    positions: Int32Builder,
    ids: GenericStringBuilder<i32>,
    references: GenericStringBuilder<i32>,
    alternates: GenericStringBuilder<i32>,
    qualities: Float32Builder,
    filters: GenericStringBuilder<i32>,
    infos: GenericStringBuilder<i32>,
    formats: GenericStringBuilder<i32>,
}

impl VCFSchemaTrait for VCFBatch {}

impl VCFBatch {
    pub fn new() -> Self {
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
        }
    }

    pub fn add(&mut self, record: &vcf::Record) {
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
}

impl BearRecordBatch for VCFBatch {
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
            Arc::new(self.vcf_schema()),
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
}

pub fn add_next_vcf_record_to_batch<R: BufRead>(
    reader: &mut noodles::vcf::reader::Reader<R>,
    header: &noodles::vcf::Header,
    n_records: Option<usize>,
) -> Option<Result<RecordBatch, ArrowError>> {
    let mut vcf_batch = VCFBatch::new();

    for _ in 0..n_records.unwrap_or(2048) {
        let mut record = noodles::vcf::Record::default();

        match reader.read_record(&header, &mut record) {
            Ok(0) => {
                let arrow_batch = vcf_batch.to_batch();

                if arrow_batch.num_rows() > 0 {
                    return Some(Ok(arrow_batch));
                } else {
                    return None;
                }
            }
            Ok(_) => {
                vcf_batch.add(&record);
            }
            Err(e) => {
                return Some(Err(ArrowError::ExternalError(Box::new(e))));
            }
        }
    }

    Some(Ok(vcf_batch.to_batch()))
}

pub fn add_next_vcf_indexed_record_to_batch<R: Read>(
    reader: &mut noodles::vcf::IndexedReader<R>,
    header: &noodles::vcf::Header,
    n_records: Option<usize>,
) -> Option<Result<RecordBatch, ArrowError>> {
    let mut vcf_batch = VCFBatch::new();

    for _ in 0..n_records.unwrap_or(2048) {
        let mut record = noodles::vcf::Record::default();

        match reader.read_record(&header, &mut record) {
            Ok(0) => {
                let arrow_batch = vcf_batch.to_batch();

                if arrow_batch.num_rows() > 0 {
                    return Some(Ok(arrow_batch));
                } else {
                    return None;
                }
            }
            Ok(_) => {
                vcf_batch.add(&record);
            }
            Err(e) => {
                return Some(Err(ArrowError::ExternalError(Box::new(e))));
            }
        }
    }

    Some(Ok(vcf_batch.to_batch()))
}
