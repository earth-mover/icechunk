//! Runtime-provided HTTP transport for virtual chunks, including WASM.
use async_trait::async_trait;
use bytes::Bytes;
use icechunk::{
    config::HttpConfig,
    format::{
        ChunkOffset,
        manifest::{
            Checksum, SecondsSinceEpoch, VirtualReferenceError, VirtualReferenceErrorKind,
        },
    },
    virtual_chunks::{HttpVirtualChunkFetcher, HttpVirtualChunkResponse},
};
use napi::{
    bindgen_prelude::{Promise, Uint8Array},
    threadsafe_function::ThreadsafeFunction,
};
use napi_derive::napi;
use std::{collections::HashMap, ops::Range};

#[napi(object, js_name = "HttpVirtualChunkRequest")]
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
#[napi(object, js_name = "HttpVirtualChunkResponse")]
pub struct JsHttpVirtualChunkResponse {
    pub data: Uint8Array,
    pub etag: Option<String>,
    /// Unix timestamp in seconds from the response Last-Modified header.
    pub last_modified: Option<u32>,
}

pub struct JsHttpVirtualChunkFetcher(
    pub ThreadsafeFunction<JsHttpVirtualChunkRequest, Promise<JsHttpVirtualChunkResponse>>,
);
impl std::fmt::Debug for JsHttpVirtualChunkFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JsHttpVirtualChunkFetcher")
    }
}
fn fetch_error(error: impl std::fmt::Display) -> VirtualReferenceError {
    VirtualReferenceError::capture(VirtualReferenceErrorKind::FetchError(
        std::io::Error::other(error.to_string()).into(),
    ))
}
#[async_trait]
impl HttpVirtualChunkFetcher for JsHttpVirtualChunkFetcher {
    async fn fetch(
        &self,
        url: &str,
        range: &Range<ChunkOffset>,
        checksum: Option<&Checksum>,
        config: &HttpConfig,
    ) -> Result<HttpVirtualChunkResponse, VirtualReferenceError> {
        const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
        if range.start > MAX_SAFE_INTEGER || range.end > MAX_SAFE_INTEGER {
            return Err(fetch_error(
                "virtual chunk byte range exceeds JavaScript's safe integer range",
            ));
        }
        let request = JsHttpVirtualChunkRequest {
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
        };
        let response = self
            .0
            .call_async(Ok(request))
            .await
            .map_err(fetch_error)?
            .await
            .map_err(fetch_error)?;
        Ok(HttpVirtualChunkResponse {
            data: Bytes::from(response.data.to_vec()),
            etag: response.etag,
            last_modified: response.last_modified,
        })
    }
}
