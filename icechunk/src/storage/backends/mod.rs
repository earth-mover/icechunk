#[cfg(not(target_arch = "wasm32"))]
pub mod object_store;
#[cfg(not(target_arch = "wasm32"))]
pub mod s3;

// Re-export implementations conditionally
#[cfg(not(target_arch = "wasm32"))]
pub use object_store::ObjectStorage;
#[cfg(not(target_arch = "wasm32"))]
pub use s3::S3Storage;
