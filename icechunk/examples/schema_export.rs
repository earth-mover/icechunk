use schemars::schema_for;

use icechunk::format::manifest::Manifest;
use icechunk::format::snapshot::Snapshot;
use icechunk::refs::RefData;

fn main() {
    println!("{}", serde_json::to_string_pretty(&schema_for!(Snapshot)).unwrap());
    println!("{}", serde_json::to_string_pretty(&schema_for!(Manifest)).unwrap());
    println!("{}", serde_json::to_string_pretty(&schema_for!(RefData)).unwrap());
}