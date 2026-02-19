// Conditional Send bound for WASM compatibility.
// Single-threaded WASM does not need `Send`, and some dependencies
// (e.g. backon's GlooTimersSleep) are not `Send` there.
// Enable `napi-send-contract` to force `Send` futures on WASM for napi compatibility checks.

use std::{future::Future, pin::Pin};

#[cfg(any(not(target_family = "wasm"), feature = "napi-send-contract"))]
pub type IcechunkBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[cfg(all(target_family = "wasm", not(feature = "napi-send-contract")))]
pub type IcechunkBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[cfg(any(not(target_family = "wasm"), feature = "napi-send-contract"))]
macro_rules! ic_boxed {
    ($fut:expr) => {
        futures::FutureExt::boxed($fut)
    };
}

#[cfg(all(target_family = "wasm", not(feature = "napi-send-contract")))]
macro_rules! ic_boxed {
    ($fut:expr) => {
        futures::FutureExt::boxed_local($fut)
    };
}

pub(crate) use ic_boxed;
