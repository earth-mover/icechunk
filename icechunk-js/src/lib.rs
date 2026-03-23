#![deny(clippy::all)]

mod config;
mod errors;
mod repository;
mod session;
mod storage;
mod store;

#[cfg(target_family = "wasm")]
#[napi::module_init]
fn init() {
    napi::create_custom_tokio_runtime(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime"),
    );
}
