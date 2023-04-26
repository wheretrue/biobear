use std::{io::BufRead, sync::Arc};

use arrow::{
    array::GenericStringBuilder,
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use noodles::fastq::Reader;

use crate::batch::BearRecordBatch;

pub struct FastqBatch {
    names: GenericStringBuilder<i32>,
    descriptions: GenericStringBuilder<i32>,
    sequences: GenericStringBuilder<i32>,
    qualities: GenericStringBuilder<i32>,
}

pub trait FastqSchemaTrait {
    fn fastq_schema(&self) -> Schema {
        Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality", DataType::Utf8, false),
        ])
    }
}

impl FastqSchemaTrait for FastqBatch {}

impl FastqBatch {
    pub fn new() -> Self {
        Self {
            names: GenericStringBuilder::<i32>::new(),
            descriptions: GenericStringBuilder::<i32>::new(),
            sequences: GenericStringBuilder::<i32>::new(),
            qualities: GenericStringBuilder::<i32>::new(),
        }
    }

    pub fn add(&mut self, record: noodles::fastq::Record) {
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
            Arc::new(self.fastq_schema()),
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

pub fn add_next_fastq_record_to_batch<R: BufRead>(
    reader: &mut Reader<R>,
    n_records: usize,
) -> Option<Result<RecordBatch, ArrowError>> {
    let mut fastq_batch = FastqBatch::new();

    for _ in 0..n_records {
        let mut record = noodles::fastq::Record::default();
        let record_read_result = reader.read_record(&mut record);

        match record_read_result {
            Ok(0) => {
                let record_batch = fastq_batch.to_batch();

                if record_batch.num_rows() == 0 {
                    return None;
                } else {
                    return Some(Ok(record_batch));
                }
            }
            Ok(_) => {
                fastq_batch.add(record);
            }
            Err(e) => return Some(Err(ArrowError::ExternalError(Box::new(e)))),
        }
    }

    Some(Ok(fastq_batch.to_batch()))
}
