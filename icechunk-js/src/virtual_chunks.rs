//! Runtime-provided HTTP transport for virtual chunks, including WASM.
#![expect(
    deprecated,
    reason = "setHttpVirtualChunkFetcher is not deprecated in JS; it is built on the core shim"
)]
use bytes::Bytes;
use icechunk::{
    config::HttpConfig,
    format::{
        ChunkOffset,
        manifest::{
            Checksum, SecondsSinceEpoch, VirtualReferenceError, VirtualReferenceErrorKind,
        },
    },
    virtual_chunks::HttpVirtualChunkResponse,
};
#[cfg(not(browser))]
use napi_derive::napi;
use std::{collections::HashMap, ops::Range};
// see the note on `Buffer` in storage.rs
#[cfg(browser)]
use serde_bytes::ByteBuf as Uint8Array;
#[cfg(not(browser))]
use {
    icechunk::virtual_chunks::HttpVirtualChunkFetcher,
    napi::{
        bindgen_prelude::{Promise, Uint8Array},
        threadsafe_function::ThreadsafeFunction,
    },
};

#[cfg_attr(not(browser), napi(object, js_name = "HttpVirtualChunkRequest"))]
#[cfg_attr(
    browser,
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct JsHttpVirtualChunkRequest {
    pub url: String,
    /// Inclusive start, exclusive end. Both must be safe JavaScript integers.
    pub range_start: f64,
    pub range_end: f64,
    pub etag: Option<String>,
    /// Unix timestamp in seconds, for If-Unmodified-Since.
    pub last_modified: Option<u32>,
    pub headers: HashMap<String, String>,
    pub options: HashMap<String, String>,
}

/// If the reference records an ETag, return the response's `etag`. If it records
/// a modification-time check, return the response's `lastModified` timestamp.
/// The resolver rejects the read if the required value is missing or fails the
/// check. Both fields are optional when the reference has no checksum.
#[cfg_attr(not(browser), napi(object, js_name = "HttpVirtualChunkResponse"))]
#[cfg_attr(
    browser,
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct JsHttpVirtualChunkResponse {
    pub data: Uint8Array,
    pub etag: Option<String>,
    /// Unix timestamp in seconds from the response Last-Modified header.
    pub last_modified: Option<u32>,
}

pub(crate) fn fetch_error(error: impl std::fmt::Display) -> VirtualReferenceError {
    VirtualReferenceError::capture(VirtualReferenceErrorKind::FetchError(
        std::io::Error::other(error.to_string()).into(),
    ))
}

impl JsHttpVirtualChunkRequest {
    pub(crate) fn new(
        url: &str,
        range: &Range<ChunkOffset>,
        checksum: Option<&Checksum>,
        config: &HttpConfig,
    ) -> Result<Self, VirtualReferenceError> {
        const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
        if range.start > MAX_SAFE_INTEGER || range.end > MAX_SAFE_INTEGER {
            return Err(fetch_error(
                "virtual chunk byte range exceeds JavaScript's safe integer range",
            ));
        }
        Ok(Self {
            url: url.to_owned(),
            range_start: range.start as f64,
            range_end: range.end as f64,
            etag: match checksum {
                Some(Checksum::ETag(etag)) => Some(etag.0.clone()),
                _ => None,
            },
            last_modified: match checksum {
                Some(Checksum::LastModified(SecondsSinceEpoch(s))) => Some(*s),
                _ => None,
            },
            headers: config.headers.clone(),
            options: config.opts.clone(),
        })
    }
}

impl From<JsHttpVirtualChunkResponse> for HttpVirtualChunkResponse {
    fn from(response: JsHttpVirtualChunkResponse) -> Self {
        HttpVirtualChunkResponse {
            data: Bytes::from(response.data.to_vec()),
            etag: response.etag,
            last_modified: response.last_modified,
        }
    }
}

/// The browser build has its own, in `browser::virtual_chunks`.
#[cfg(not(browser))]
pub(crate) struct JsHttpVirtualChunkFetcher(
    pub(crate)  ThreadsafeFunction<
        JsHttpVirtualChunkRequest,
        Promise<JsHttpVirtualChunkResponse>,
    >,
);

#[cfg(not(browser))]
impl std::fmt::Debug for JsHttpVirtualChunkFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JsHttpVirtualChunkFetcher")
    }
}

#[cfg(not(browser))]
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
        let response = self
            .0
            .call_async(Ok(request))
            .await
            .map_err(fetch_error)?
            .await
            .map_err(fetch_error)?;
        Ok(response.into())
    }
}
