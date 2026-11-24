#![deny(clippy::all)]

//! JavaScript bindings for icechunk.
//!
//! Node (native) is bound with napi; the browser (`wasm32-unknown-unknown`,
//! `cfg(browser)`) with wasm-bindgen. The classes and plain-object types are
//! shared. The napi method shims sit next to the types, the wasm-bindgen ones
//! in [`browser`].

mod config;
mod errors;
mod repository;
mod session;
mod storage;
mod store;
mod virtual_chunks;

#[cfg(browser)]
mod browser;
