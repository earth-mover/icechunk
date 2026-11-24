#![expect(
    deprecated,
    reason = "setHttpVirtualChunkFetcher is not deprecated in JS; it is built on the core shim"
)]

use std::ops::Range;

use icechunk::{
    config::HttpConfig,
    format::{
        ChunkOffset,
        manifest::{Checksum, VirtualReferenceError},
    },
    virtual_chunks::{HttpVirtualChunkFetcher, HttpVirtualChunkResponse},
};
use js_sys::{Function, Promise};
use send_wrapper::SendWrapper;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;

use super::{from_js, js_message, to_js};
use crate::virtual_chunks::{
    JsHttpVirtualChunkRequest, JsHttpVirtualChunkResponse, fetch_error,
};

/// The JS callback registered with `setHttpVirtualChunkFetcher`. See
/// `JsCallbackStorage` for why it sits in a `SendWrapper`.
pub(crate) struct JsHttpVirtualChunkFetcher(pub(crate) SendWrapper<Function>);

impl std::fmt::Debug for JsHttpVirtualChunkFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JsHttpVirtualChunkFetcher")
    }
}

#[icechunk_storage::maybe_send_async_trait]
impl HttpVirtualChunkFetcher for JsHttpVirtualChunkFetcher {
    async fn fetch(
        &self,
        url: &str,
        range: &Range<ChunkOffset>,
        checksum: Option<&Checksum>,
        config: &HttpConfig,
        // the JS callback owns the transport; browsers ignore a User-Agent set
        // on fetch, and adding it would trigger CORS preflights
        _user_agent: &str,
    ) -> Result<HttpVirtualChunkResponse, VirtualReferenceError> {
        let request = JsHttpVirtualChunkRequest::new(url, range, checksum, config)?;
        let request = to_js(&request).map_err(|e| fetch_error(js_message(&e)))?;
        // error-first, like the napi build
        let returned = self
            .0
            .call2(&JsValue::UNDEFINED, &JsValue::NULL, &request)
            .map_err(|e| fetch_error(js_message(&e)))?;
        let response = JsFuture::from(Promise::resolve(&returned))
            .await
            .map_err(|e| fetch_error(js_message(&e)))?;
        let response: JsHttpVirtualChunkResponse =
            from_js(response).map_err(|e| fetch_error(js_message(&e)))?;
        Ok(response.into())
    }
}
