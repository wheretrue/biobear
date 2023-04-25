use std::{io::BufRead, sync::Arc};

use arrow::{
    array::{GenericStringBuilder, Int32Builder},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use noodles::{bgzf, sam};

use crate::batch::BearRecordBatch;

pub trait BamSchemaTrait {
    fn bam_schema(&self) -> Schema {
        Schema::new(vec![
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
        ])
    }
}

pub struct BamBatch {
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
}

impl BamBatch {
    pub fn new() -> Self {
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
        }
    }

    pub fn add(&mut self, record: sam::alignment::Record, header: &sam::Header) {
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
}

impl BamSchemaTrait for BamBatch {}

impl BearRecordBatch for BamBatch {
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
            Arc::new(self.bam_schema()),
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
}

pub fn add_next_bam_record_to_batch<R: BufRead>(
    reader: &mut noodles::bam::reader::Reader<R>,
    header: &noodles::sam::Header,
    n_records: Option<usize>,
) -> Option<Result<RecordBatch, ArrowError>> {
    let mut bam_batch = BamBatch::new();
    for _ in 0..n_records.unwrap_or(2048) {
        let mut record = noodles::sam::alignment::Record::default();

        match reader.read_record(header, &mut record) {
            Ok(0) => {
                let arrow_batch = bam_batch.to_batch();

                if arrow_batch.num_rows() > 0 {
                    return Some(Ok(arrow_batch));
                } else {
                    return None;
                }
            }
            Ok(_) => {
                bam_batch.add(record, header);
            }
            Err(e) => {
                return Some(Err(ArrowError::ExternalError(Box::new(e))));
            }
        }
    }

    Some(Ok(bam_batch.to_batch()))
}

pub fn add_next_bam_indexed_record_to_batch<R: BufRead>(
    reader: &mut noodles::bam::IndexedReader<bgzf::Reader<R>>,
    header: &noodles::sam::Header,
    n_records: Option<usize>,
) -> Option<Result<RecordBatch, ArrowError>> {
    let mut bam_batch = BamBatch::new();
    for _ in 0..n_records.unwrap_or(2048) {
        let mut record = noodles::sam::alignment::Record::default();

        match reader.read_record(header, &mut record) {
            Ok(0) => {
                let arrow_batch = bam_batch.to_batch();

                if arrow_batch.num_rows() > 0 {
                    return Some(Ok(arrow_batch));
                } else {
                    return None;
                }
            }
            Ok(_) => {
                bam_batch.add(record, header);
            }
            Err(e) => {
                return Some(Err(ArrowError::ExternalError(Box::new(e))));
            }
        }
    }

    Some(Ok(bam_batch.to_batch()))
}
