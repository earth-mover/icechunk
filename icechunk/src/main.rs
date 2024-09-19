use schemars::schema_for;

use icechunk::format::manifest::Manifest;
use icechunk::format::snapshot::Snapshot;

fn main() {
    let snapshot_schema = schema_for!(Snapshot);
    println!("{}", serde_json::to_string_pretty(&snapshot_schema).unwrap());
    let manifest_schema = schema_for!(Manifest);
    println!("{}", serde_json::to_string_pretty(&manifest_schema).unwrap());
}