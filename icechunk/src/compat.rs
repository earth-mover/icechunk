// Conditional Send bound for WASM compatibility.
// Single-threaded WASM does not need `Send`, and some dependencies
// (e.g. backon's GlooTimersSleep) are not `Send` there.
// WASI builds used by napi require `Send` futures.

use std::{future::Future, pin::Pin};

#[cfg(any(not(target_family = "wasm"), target_os = "wasi"))]
pub type IcechunkBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[cfg(all(target_family = "wasm", not(target_os = "wasi")))]
pub type IcechunkBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[cfg(any(not(target_family = "wasm"), target_os = "wasi"))]
macro_rules! ic_boxed {
    ($fut:expr) => {
        futures::FutureExt::boxed($fut)
    };
}

#[cfg(all(target_family = "wasm", not(target_os = "wasi")))]
macro_rules! ic_boxed {
    ($fut:expr) => {
        futures::FutureExt::boxed_local($fut)
    };
}

pub(crate) use ic_boxed;
