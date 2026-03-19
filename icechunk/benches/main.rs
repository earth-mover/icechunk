#![allow(clippy::unwrap_used)]

mod helpers;

#[allow(dead_code)]
mod asset_manager;
#[allow(dead_code)]
mod manifest;

fn main() {
    helpers::init_samply();

    manifest::manifest_benches();
    asset_manager::asset_manager_benches();

    criterion::Criterion::default().configure_from_args().final_summary();
}
