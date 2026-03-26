#![allow(clippy::unwrap_used)]

mod helpers;

mod asset_manager;
mod manifest;

fn main() {
    let _guard = helpers::init_tracing();

    manifest::manifest_benches();
    asset_manager::asset_manager_benches();

    criterion::Criterion::default().configure_from_args().final_summary();
}
