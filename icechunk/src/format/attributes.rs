use arrow::array::RecordBatch;

use super::BatchLike;

#[derive(Debug, PartialEq)]
pub struct AttributesTable {
    pub batch: RecordBatch,
}

impl BatchLike for AttributesTable {
    fn get_batch(&self) -> &RecordBatch {
        &self.batch
    }
}
