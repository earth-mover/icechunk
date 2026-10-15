#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_fun_call
)]

mod common;
mod test_attribution;
mod test_concurrency;
mod test_distributed_writes;
#[cfg(all(feature = "fs", unix))]
mod test_filesystem_storage;
mod test_flaky_connections;
mod test_gc;
mod test_key_layout;
#[cfg(feature = "shuttle")]
mod test_shuttle;
mod test_stats;
mod test_storage;
mod test_virtual_refs;

/// `block_in_place` panics on a current-thread runtime, so this fails if
/// `tokio_test` drops its arguments.
#[icechunk_macros::tokio_test(flavor = "multi_thread")]
async fn tokio_test_passes_its_arguments_to_tokio() {
    tokio::task::block_in_place(|| {});
}
