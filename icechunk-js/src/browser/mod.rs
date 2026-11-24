//! wasm-bindgen bindings for the browser (`wasm32-unknown-unknown`).
//!
//! These mirror the napi API in `index.d.ts`, so the same TypeScript types
//! apply. wasm-bindgen cannot export an `async fn` that borrows `self`, so
//! async methods clone what they need and return a `Promise` from [`spawn`].
//!
//! Getters cannot wait for a lock (a blocking read would spin forever), so
//! `Session` getters report the last state they saw while an operation holds
//! the session; see `session.rs`.

mod repository;
mod session;
mod storage;
mod store;
mod virtual_chunks;

#[cfg(test)]
mod tests;

use std::future::Future;

use js_sys::Promise;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{RwLock, RwLockReadGuard};
use wasm_bindgen::{JsCast as _, JsValue};

use crate::errors::chain_message;

pub(crate) type JsResult<T> = Result<T, JsValue>;

/// A JS `Error` with `message`.
pub(crate) fn js_error(message: impl AsRef<str>) -> JsValue {
    js_sys::Error::new(message.as_ref()).into()
}

pub(crate) trait IntoJsResult<T> {
    /// Map an icechunk error to a JS `Error` carrying its whole cause chain.
    fn js(self) -> JsResult<T>;
}

impl<T, E: std::error::Error + 'static> IntoJsResult<T> for Result<T, E> {
    fn js(self) -> JsResult<T> {
        self.map_err(|e| js_error(chain_message(&e)))
    }
}

/// The message of a thrown or rejected JS value.
pub(crate) fn js_message(value: &JsValue) -> String {
    if let Some(error) = value.dyn_ref::<js_sys::Error>() {
        String::from(error.message())
    } else if let Some(s) = value.as_string() {
        s
    } else {
        format!("{value:?}")
    }
}

/// Convert to a plain JS value, with maps as objects, like napi does.
pub(crate) fn to_js<T: Serialize + ?Sized>(value: &T) -> JsResult<JsValue> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true))
        .map_err(|e| js_error(e.to_string()))
}

/// `null` for `None`, like napi's `T | null` returns.
pub(crate) fn to_js_nullable<T: Serialize>(value: Option<T>) -> JsResult<JsValue> {
    value.map_or(Ok(JsValue::NULL), |v| to_js(&v))
}

pub(crate) fn from_js<T: DeserializeOwned>(value: JsValue) -> JsResult<T> {
    serde_wasm_bindgen::from_value(value).map_err(|e| js_error(e.to_string()))
}

/// `None` for `undefined` or `null`, like napi's optional arguments.
pub(crate) fn from_js_optional<T: DeserializeOwned>(
    value: JsValue,
) -> JsResult<Option<T>> {
    if value.is_null() || value.is_undefined() {
        Ok(None)
    } else {
        from_js(value).map(Some)
    }
}

/// Run `fut` on the JS event loop and return its result as a `Promise`.
pub(crate) fn spawn(fut: impl Future<Output = JsResult<JsValue>> + 'static) -> Promise {
    wasm_bindgen_futures::future_to_promise(fut)
}

/// [`spawn`] for operations that resolve to `undefined`.
pub(crate) fn spawn_void(fut: impl Future<Output = JsResult<()>> + 'static) -> Promise {
    spawn(async { fut.await.map(|()| JsValue::UNDEFINED) })
}

#[wasm_bindgen::prelude::wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = error)]
    fn console_error(message: &str);
}

/// Report panics on the console. Without this a panic surfaces only as
/// `RuntimeError: unreachable`, with no message.
#[wasm_bindgen::prelude::wasm_bindgen(start)]
fn start() {
    std::panic::set_hook(Box::new(|info| {
        console_error(&format!("icechunk panicked: {info}"))
    }));
}

/// A read lock for a synchronous getter, or an error if an operation holds
/// the write lock.
pub(crate) fn read_now<T>(lock: &RwLock<T>) -> JsResult<RwLockReadGuard<'_, T>> {
    lock.try_read().map_err(|_| {
        js_error("busy: another operation is using this object; await it first")
    })
}
