//! C FFI bindings for the Icechunk Zarr store.

#![allow(clippy::missing_safety_doc)]

pub mod error;
mod ffi;
mod runtime;
pub mod storage;
pub mod store;
