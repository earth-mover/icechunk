#![allow(clippy::unwrap_used)]

use criterion::criterion_main;

mod helpers;

#[allow(dead_code)]
mod asset_manager;
#[allow(dead_code)]
mod manifest;

criterion_main!(manifest::manifest_benches, asset_manager::asset_manager_benches);
