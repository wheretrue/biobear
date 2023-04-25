// Implement a trait for a type with a `to_batch` method that returns an arrow record batch

use arrow::{ipc::writer::FileWriter, record_batch::RecordBatch};

pub trait BearRecordBatch {
    fn to_batch(&mut self) -> RecordBatch;

    // serialize the record batch to an IPC format
    fn serialize(&mut self) -> Vec<u8> {
        let batch = self.to_batch();

        let mut writer = FileWriter::try_new(Vec::new(), &batch.schema()).unwrap();

        writer.write(&batch).unwrap();
        writer.finish().unwrap();

        writer.into_inner().unwrap()
    }
}
