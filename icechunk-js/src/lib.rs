#![deny(clippy::all)]

//! JavaScript bindings for icechunk.
//!
//! Node (native and `wasm32-wasip1-threads`) is bound with napi; the browser
//! (`wasm32-unknown-unknown`, `cfg(browser)`) with wasm-bindgen. The classes and
//! the plain-object types are declared once and shared. Each binding has its
//! own method shims: the napi ones next to the types, the wasm-bindgen ones in
//! [`browser`].

mod config;
mod errors;
mod repository;
mod session;
mod storage;
mod store;
mod virtual_chunks;

#[cfg(browser)]
mod browser;
