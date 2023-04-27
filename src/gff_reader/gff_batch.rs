use std::{io::BufRead, str::FromStr, sync::Arc};

use arrow::{
    array::{Float32Builder, GenericStringBuilder, Int64Builder},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use noodles::gff::Line;

use crate::batch::BearRecordBatch;

pub trait GFFSchemaTrait {
    fn gff_schema(&self) -> Schema {
        Schema::new(vec![
            Field::new("seqname", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, true),
            Field::new("feature", DataType::Utf8, false),
            Field::new("start", DataType::Int64, false),
            Field::new("end", DataType::Int64, false),
            Field::new("score", DataType::Float32, true),
            Field::new("strand", DataType::Utf8, false),
            Field::new("phase", DataType::Utf8, true),
            Field::new("attributes", DataType::Utf8, true),
        ])
    }
}

pub struct GFFBatch {
    seqnames: GenericStringBuilder<i32>,
    sources: GenericStringBuilder<i32>,
    feature_types: GenericStringBuilder<i32>,
    starts: Int64Builder,
    ends: Int64Builder,
    scores: Float32Builder,
    strands: GenericStringBuilder<i32>,
    phases: GenericStringBuilder<i32>,
    attributes: GenericStringBuilder<i32>,
}

impl GFFSchemaTrait for GFFBatch {}

impl GFFBatch {
    pub fn new() -> Self {
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
        }
    }

    pub fn add(&mut self, record: noodles::gff::Record) {
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
}

impl BearRecordBatch for GFFBatch {
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
            Arc::new(self.gff_schema()),
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
}

pub fn add_next_gff_record_to_batch<R: BufRead>(
    reader: &mut noodles::gff::Reader<R>,
    n_records: Option<usize>,
) -> Option<Result<RecordBatch, ArrowError>> {
    let mut gff_batch = GFFBatch::new();
    for _ in 0..n_records.unwrap_or(2048) {
        let mut buffer = String::new();

        match reader.read_line(&mut buffer) {
            Ok(0) => {
                let arrow_batch = gff_batch.to_batch();

                if arrow_batch.num_rows() == 0 {
                    return None;
                }

                return Some(Ok(arrow_batch));
            }
            Ok(_) => {
                let line_result = Line::from_str(&buffer);

                match line_result {
                    Ok(line) => match line {
                        Line::Record(record) => {
                            gff_batch.add(record);
                        }
                        _ => {}
                    },
                    Err(e) => {
                        return Some(Err(ArrowError::ExternalError(Box::new(
                            std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()),
                        ))))
                    }
                }
            }
            Err(e) => return Some(Err(ArrowError::from(e))),
        }
    }

    Some(Ok(gff_batch.to_batch()))
}
