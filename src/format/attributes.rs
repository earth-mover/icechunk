use arrow::array::RecordBatch;

#[derive(Debug, PartialEq)]
pub struct AttributesTable {
    pub batch: RecordBatch,
}
