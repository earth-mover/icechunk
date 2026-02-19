// Conditional Send bound for WASM compatibility.
// WASM is single-threaded so Send is not needed, and some dependencies
// (e.g. backon's GlooTimersSleep) don't implement Send on WASM.

use std::{future::Future, pin::Pin};

#[cfg(not(target_family = "wasm"))]
pub type IcechunkBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[cfg(target_family = "wasm")]
pub type IcechunkBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

#[cfg(not(target_family = "wasm"))]
macro_rules! ic_boxed {
    ($fut:expr) => {
        futures::FutureExt::boxed($fut)
    };
}

#[cfg(target_family = "wasm")]
macro_rules! ic_boxed {
    ($fut:expr) => {
        futures::FutureExt::boxed_local($fut)
    };
}

pub(crate) use ic_boxed;
