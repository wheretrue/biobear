use std::{io::BufRead, sync::Arc};

use arrow::{
    array::{GenericListBuilder, GenericStringBuilder, StringBuilder, StructBuilder},
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};

use gb_io::{reader, seq::Seq};

use crate::batch::BearRecordBatch;

pub trait GenbankSchemaTrait {
    fn genbank_schema(&self) -> Schema {
        let kind_field = Field::new("kind", DataType::Utf8, false);
        let location_field = Field::new("location", DataType::Utf8, false);
        let qualifiers_field = Field::new("qualifiers", DataType::Utf8, false);

        let fields = Fields::from(vec![kind_field, location_field, qualifiers_field]);
        let feature_field = Field::new("item", DataType::Struct(fields), true);

        let comment_field = Field::new("item", DataType::Utf8, true);

        let schema = Schema::new(vec![
            Field::new("sequence", DataType::Utf8, false),
            Field::new("accession", DataType::Utf8, true),
            Field::new("comments", DataType::List(Arc::new(comment_field)), true),
            Field::new("contig", DataType::Utf8, true),
            Field::new("date", DataType::Utf8, true),
            Field::new("dblink", DataType::Utf8, true),
            Field::new("definition", DataType::Utf8, true),
            Field::new("division", DataType::Utf8, true),
            Field::new("keywords", DataType::Utf8, true),
            Field::new("molecule_type", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("source", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
            Field::new("topology", DataType::Utf8, true),
            Field::new("features", DataType::List(Arc::new(feature_field)), true),
        ]);

        schema
    }
}

pub struct GenbankBatch {
    sequence: GenericStringBuilder<i32>,
    accession: GenericStringBuilder<i32>,
    comments: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    contig: GenericStringBuilder<i32>,
    date: GenericStringBuilder<i32>,
    dblink: GenericStringBuilder<i32>,
    definition: GenericStringBuilder<i32>,
    division: GenericStringBuilder<i32>,
    keywords: GenericStringBuilder<i32>,
    molecule_type: GenericStringBuilder<i32>,
    name: GenericStringBuilder<i32>,
    source: GenericStringBuilder<i32>,
    version: GenericStringBuilder<i32>,
    topology: GenericStringBuilder<i32>,
    features: GenericListBuilder<i32, StructBuilder>,
}

impl GenbankSchemaTrait for GenbankBatch {}

impl GenbankBatch {
    pub fn new() -> Self {
        let kind_builder = GenericStringBuilder::<i32>::new();
        let location_builder = GenericStringBuilder::<i32>::new();
        let qualifiers_builder = GenericStringBuilder::<i32>::new();

        let kind_field = Field::new("kind", DataType::Utf8, false);
        let location_field = Field::new("location", DataType::Utf8, false);
        let qualifiers_field = Field::new("qualifiers", DataType::Utf8, false);

        let fields = Fields::from(vec![kind_field, location_field, qualifiers_field]);

        let feature_builder = StructBuilder::new(
            fields,
            vec![
                Box::new(kind_builder),
                Box::new(location_builder),
                Box::new(qualifiers_builder),
            ],
        );

        Self {
            sequence: GenericStringBuilder::new(),
            accession: GenericStringBuilder::new(),
            comments: GenericListBuilder::new(GenericStringBuilder::new()),
            contig: GenericStringBuilder::new(),
            date: GenericStringBuilder::new(),
            dblink: GenericStringBuilder::new(),
            definition: GenericStringBuilder::new(),
            division: GenericStringBuilder::new(),
            keywords: GenericStringBuilder::new(),
            molecule_type: GenericStringBuilder::new(),
            name: GenericStringBuilder::new(),
            source: GenericStringBuilder::new(),
            version: GenericStringBuilder::new(),
            topology: GenericStringBuilder::new(),
            features: GenericListBuilder::new(feature_builder),
        }
    }

    pub fn add(&mut self, record: &Seq) {
        let seq_str = std::str::from_utf8(&record.seq).unwrap();
        self.sequence.append_value(seq_str);

        self.accession.append_option(record.accession.as_ref());

        if record.comments.len() > 0 {
            let values = self.comments.values();

            record.comments.iter().for_each(|comment| {
                values.append_value(comment);
            });

            self.comments.append(true);
        } else {
            self.comments.append_null();
        }

        self.contig
            .append_option(record.contig.as_ref().map(|contig| contig.to_string()));
        self.date
            .append_option(record.date.as_ref().map(|date| date.to_string()));
        self.dblink.append_option(record.dblink.as_ref());
        self.definition.append_option(record.definition.as_ref());
        self.division.append_value(record.division.to_string());
        self.keywords.append_option(record.keywords.as_ref());
        self.molecule_type
            .append_option(record.molecule_type.as_ref());

        self.name.append_option(record.name.as_ref());
        self.source.append_option(
            record
                .source
                .as_ref()
                .map(|source| source.source.to_string()),
        );

        self.version.append_option(record.version.as_ref());
        self.topology.append_value(record.topology.to_string());

        let seq_features = &record.features;

        let feature_values = self.features.values();
        for feature in seq_features {
            let kind = feature.kind.to_string();
            let location = feature.location.to_string();
            // let qualifiers = feature.qualifiers;

            feature_values
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(kind);

            feature_values
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(location);

            feature_values
                .field_builder::<StringBuilder>(2)
                .unwrap()
                .append_value("qualifiers");

            feature_values.append(true);
        }
        self.features.append(true);
    }
}

impl BearRecordBatch for GenbankBatch {
    fn to_batch(&mut self) -> RecordBatch {
        let sequence = self.sequence.finish();
        let accession = self.accession.finish();
        let comments = self.comments.finish();
        let contig = self.contig.finish();
        let date = self.date.finish();
        let dblink = self.dblink.finish();
        let definition = self.definition.finish();
        let division = self.division.finish();
        let keywords = self.keywords.finish();
        let molecule_type = self.molecule_type.finish();
        let name = self.name.finish();
        let source = self.source.finish();
        let version = self.version.finish();
        let topology = self.topology.finish();
        let features = self.features.finish();

        RecordBatch::try_new(
            Arc::new(self.genbank_schema()),
            vec![
                Arc::new(sequence),
                Arc::new(accession),
                Arc::new(comments),
                Arc::new(contig),
                Arc::new(date),
                Arc::new(dblink),
                Arc::new(definition),
                Arc::new(division),
                Arc::new(keywords),
                Arc::new(molecule_type),
                Arc::new(name),
                Arc::new(source),
                Arc::new(version),
                Arc::new(topology),
                Arc::new(features),
            ],
        )
        .unwrap()
    }
}

pub fn add_next_genbank_record_to_batch<R: BufRead>(
    reader: &mut reader::SeqReader<R>,
    n_records: Option<usize>,
) -> Option<Result<RecordBatch, ArrowError>> {
    let mut genbank_batch = GenbankBatch::new();

    for _ in 0..n_records.unwrap_or(2048) {
        let record = reader.next();

        if record.is_none() {
            let record_batch = genbank_batch.to_batch();

            if record_batch.num_rows() == 0 {
                return None;
            }

            return Some(Ok(record_batch));
        }

        let unwrapped_record = record.unwrap();

        match unwrapped_record {
            Ok(record) => {
                genbank_batch.add(&record);
            }
            Err(e) => {
                return Some(Err(ArrowError::ExternalError(Box::new(e))));
            }
        }
    }

    Some(Ok(genbank_batch.to_batch()))
}
