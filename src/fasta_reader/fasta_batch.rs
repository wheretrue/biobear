use std::{io::BufRead, sync::Arc};

use arrow::{
    array::GenericStringBuilder,
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use noodles::fasta::Reader;

use crate::batch::BearRecordBatch;

pub trait FastaSchemaTrait {
    fn fasta_schema(&self) -> Schema {
        Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
        ])
    }
}

pub struct FastaBatch {
    names: GenericStringBuilder<i32>,
    descriptions: GenericStringBuilder<i32>,
    sequences: GenericStringBuilder<i32>,
}

impl FastaSchemaTrait for FastaBatch {}

impl FastaBatch {
    pub fn new() -> Self {
        Self {
            names: GenericStringBuilder::<i32>::new(),
            descriptions: GenericStringBuilder::<i32>::new(),
            sequences: GenericStringBuilder::<i32>::new(),
        }
    }

    pub fn add_from_parts(&mut self, name: &str, description: Option<&str>, sequence: &str) {
        self.names.append_value(name);

        match description {
            Some(description) => self.descriptions.append_value(description),
            None => self.descriptions.append_null(),
        }

        self.sequences.append_value(sequence);
    }
}

impl BearRecordBatch for FastaBatch {
    fn to_batch(&mut self) -> RecordBatch {
        let names = self.names.finish();
        let descriptions = self.descriptions.finish();
        let sequences = self.sequences.finish();

        RecordBatch::try_new(
            Arc::new(self.fasta_schema()),
            vec![Arc::new(names), Arc::new(descriptions), Arc::new(sequences)],
        )
        .unwrap()
    }
}

pub fn add_next_record_to_batch<R: BufRead>(
    reader: &mut Reader<R>,
    n_records: usize,
) -> Option<Result<RecordBatch, ArrowError>> {
    let mut fasta_batch = FastaBatch::new();

    for _ in 0..n_records {
        let mut buf = String::new();
        let mut sequence = Vec::new();

        match reader.read_definition(&mut buf) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    let ii = fasta_batch.to_batch();

                    if ii.num_rows() == 0 {
                        return None;
                    }

                    return Some(Ok(ii));
                }
            }
            Err(e) => return Some(Err(ArrowError::ExternalError(Box::new(e)))),
        }

        match reader.read_sequence(&mut sequence) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    let ii = fasta_batch.to_batch();

                    if ii.num_rows() == 0 {
                        return None;
                    }

                    return Some(Ok(ii));
                }
            }
            Err(e) => return Some(Err(ArrowError::ExternalError(Box::new(e)))),
        }

        let sequence_str = std::str::from_utf8(&sequence).unwrap();

        match buf.strip_prefix(">") {
            None => {
                return Some(Err(ArrowError::ExternalError(Box::new(
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid FASTA record"),
                ))))
            }
            Some(definition) => match definition.split_once(" ") {
                Some((id, description)) => {
                    fasta_batch.add_from_parts(id, Some(description), sequence_str);
                }
                None => fasta_batch.add_from_parts(definition, None, sequence_str),
            },
        }
    }

    Some(Ok(fasta_batch.to_batch()))
}
