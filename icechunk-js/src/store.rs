use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use icechunk::format::ByteRange;
use icechunk::store::{Store, StoreErrorKind};
use napi::bindgen_prelude::{Buffer, Uint8Array};
use napi_derive::napi;

use crate::errors::IntoNapiResult;
use crate::session::JsSession;

/// Range query matching zarrita's RangeQuery type:
///   { offset: number, length: number } | { suffixLength: number }
#[napi(object, js_name = "RangeQuery")]
#[derive(Clone, Debug)]
pub struct JsRangeQuery {
    pub offset: Option<f64>,
    pub length: Option<f64>,
    pub suffix_length: Option<f64>,
}

/// On WASM, some icechunk futures (e.g. from `get_chunk_reader`, `get_chunk_writer`)
/// are not `Send` because WASM is single-threaded. However, napi's async runtime
/// requires `Send` futures. This wrapper unsafely implements `Send` on WASM where
/// it is safe because there is only one thread.
///
/// We wrap the `Arc<Store>` so that all futures produced from it are considered Send.
#[cfg(target_family = "wasm")]
struct SendStore(Arc<Store>);

#[cfg(target_family = "wasm")]
// SAFETY: WASM is single-threaded, so Send is not actually needed
unsafe impl Send for SendStore {}
// SAFETY: WASM is single-threaded, so Sync is not actually needed
#[cfg(target_family = "wasm")]
unsafe impl Sync for SendStore {}

#[cfg(not(target_family = "wasm"))]
use chrono::{DateTime, Utc};
#[cfg(not(target_family = "wasm"))]
use icechunk::format::manifest::{
    Checksum, SecondsSinceEpoch, VirtualChunkLocation, VirtualChunkRef,
};
#[cfg(not(target_family = "wasm"))]
use icechunk::format::{ChunkIndices, Path};
#[cfg(not(target_family = "wasm"))]
use icechunk::storage::ETag;
#[cfg(not(target_family = "wasm"))]
use icechunk::store::SetVirtualRefsResult;

#[cfg(not(target_family = "wasm"))]
/// Create a checksum from either an etag string or a last-modified datetime
/// etag_checksum takes precedence if both are provided
fn create_checksum(
    etag: Option<String>,
    last_modified: Option<DateTime<Utc>>,
) -> Option<Checksum> {
    etag.map(|e| Checksum::ETag(ETag(e))).or_else(|| {
        last_modified
            .map(|dt| Checksum::LastModified(SecondsSinceEpoch(dt.timestamp() as u32)))
    })
}

/// Specification for a virtual chunk reference
#[cfg(not(target_family = "wasm"))]
#[napi(object, js_name = "VirtualChunkSpec")]
#[derive(Clone, Debug)]
pub struct JsVirtualChunkSpec {
    pub index: Vec<u32>,
    pub location: String,
    pub offset: i64,
    pub length: i64,
    pub etag_checksum: Option<String>,
    /// Last modified datetime (accepts JS Date object)
    pub last_modified: Option<DateTime<Utc>>,
}

#[napi(js_name = "Store")]
pub struct JsStore(pub(crate) Arc<Store>);

/// Strip leading slash from key to be compatible with zarrita's AbsolutePath type.
/// zarrita passes keys like "/zarr.json" but icechunk expects "zarr.json".
fn normalize_key(key: &str) -> &str {
    key.strip_prefix('/').unwrap_or(key)
}

#[napi]
impl JsStore {
    #[napi]
    pub async fn get(&self, key: String) -> napi::Result<Option<Uint8Array>> {
        let key = normalize_key(&key);
        match self.0.get(key, &ByteRange::ALL).await {
            Ok(bytes) => Ok(Some(Uint8Array::from(bytes.to_vec()))),
            Err(e) if matches!(e.kind, StoreErrorKind::NotFound(_)) => Ok(None),
            Err(e) => Err(napi::Error::from_reason(e.to_string())),
        }
    }

    /// Fetch a byte range from a key.
    ///
    /// Accepts zarrita's RangeQuery format:
    ///   { offset: number, length: number } - fetch length bytes starting at offset
    ///   { suffixLength: number } - fetch the last suffixLength bytes
    #[napi]
    pub async fn get_range(
        &self,
        key: String,
        range: JsRangeQuery,
    ) -> napi::Result<Option<Uint8Array>> {
        let byte_range = if let Some(suffix) = range.suffix_length {
            ByteRange::Last(suffix as u64)
        } else {
            let offset = range.offset.unwrap_or(0.0) as u64;
            match range.length {
                Some(len) => ByteRange::from_offset_with_length(offset, len as u64),
                None => ByteRange::from_offset(offset),
            }
        };
        let key = normalize_key(&key);
        match self.0.get(key, &byte_range).await {
            Ok(bytes) => Ok(Some(Uint8Array::from(bytes.to_vec()))),
            Err(e) if matches!(e.kind, StoreErrorKind::NotFound(_)) => Ok(None),
            Err(e) => Err(napi::Error::from_reason(e.to_string())),
        }
    }

    #[napi]
    pub async fn set(&self, key: String, value: Buffer) -> napi::Result<()> {
        let key = normalize_key(&key);
        let bytes = Bytes::from(value.to_vec());
        self.0.set(key, bytes).await.map_napi_err()
    }

    #[napi]
    pub async fn exists(&self, key: String) -> napi::Result<bool> {
        self.0.exists(normalize_key(&key)).await.map_napi_err()
    }

    #[napi]
    pub async fn delete(&self, key: String) -> napi::Result<()> {
        self.0.delete(normalize_key(&key)).await.map_napi_err()
    }

    #[napi]
    pub async fn list(&self) -> napi::Result<Vec<String>> {
        let stream = self.0.list().await.map_napi_err()?;
        let keys: Vec<String> = stream.try_collect().await.map_napi_err()?;
        Ok(keys)
    }

    #[napi]
    pub async fn list_prefix(&self, prefix: String) -> napi::Result<Vec<String>> {
        let stream = self.0.list_prefix(normalize_key(&prefix)).await.map_napi_err()?;
        let keys: Vec<String> = stream.try_collect().await.map_napi_err()?;
        Ok(keys)
    }

    #[napi]
    pub async fn list_dir(&self, prefix: String) -> napi::Result<Vec<String>> {
        let stream = self.0.list_dir(normalize_key(&prefix)).await.map_napi_err()?;
        let keys: Vec<String> = stream.try_collect().await.map_napi_err()?;
        Ok(keys)
    }

    #[napi(getter)]
    pub fn supports_writes(&self) -> bool {
        true
    }

    #[napi(getter)]
    pub fn supports_deletes(&self) -> bool {
        true
    }

    #[napi(getter)]
    pub fn supports_listing(&self) -> bool {
        true
    }

    #[napi(getter)]
    pub async fn read_only(&self) -> bool {
        self.0.read_only().await
    }

    #[napi(getter)]
    pub fn session(&self) -> JsSession {
        JsSession::new_from_arc(self.0.session())
    }

    #[napi]
    pub async fn is_empty(&self, prefix: String) -> napi::Result<bool> {
        self.0.is_empty(normalize_key(&prefix)).await.map_napi_err()
    }

    #[napi]
    pub async fn clear(&self) -> napi::Result<()> {
        self.0.clear().await.map_napi_err()
    }

    #[napi]
    pub async fn set_if_not_exists(
        &self,
        key: String,
        value: Buffer,
    ) -> napi::Result<()> {
        let key = normalize_key(&key);
        let bytes = Bytes::from(value.to_vec());
        self.0.set_if_not_exists(key, bytes).await.map_napi_err()
    }

    #[napi]
    pub async fn delete_dir(&self, prefix: String) -> napi::Result<()> {
        self.0.delete_dir(normalize_key(&prefix)).await.map_napi_err()
    }

    #[napi]
    pub async fn getsize(&self, key: String) -> napi::Result<i64> {
        let size = self.0.getsize(normalize_key(&key)).await.map_napi_err()?;
        Ok(size as i64)
    }

    #[napi]
    pub async fn getsize_prefix(&self, prefix: String) -> napi::Result<i64> {
        let size = self.0.getsize_prefix(normalize_key(&prefix)).await.map_napi_err()?;
        Ok(size as i64)
    }
}

#[cfg(not(target_family = "wasm"))]
#[napi]
impl JsStore {
    /// Set a single virtual reference to a chunk
    ///
    /// For checksum validation, provide either etag_checksum (string) or last_modified (JS Date object).
    /// If both are provided, etag_checksum takes precedence.
    #[napi]
    #[allow(clippy::too_many_arguments)]
    pub async fn set_virtual_ref(
        &self,
        key: String,
        location: String,
        offset: i64,
        length: i64,
        etag_checksum: Option<String>,
        last_modified: Option<DateTime<Utc>>,
        validate_container: bool,
    ) -> napi::Result<()> {
        let location = VirtualChunkLocation::from_url(location.as_str())
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let checksum = create_checksum(etag_checksum, last_modified);
        let virtual_ref = VirtualChunkRef {
            location,
            offset: offset as u64,
            length: length as u64,
            checksum,
        };
        self.0
            .set_virtual_ref(normalize_key(&key), virtual_ref, validate_container)
            .await
            .map_napi_err()
    }

    /// Set multiple virtual references for the same array
    /// Returns the indices of failed chunk references if any
    #[napi]
    pub async fn set_virtual_refs(
        &self,
        array_path: String,
        chunks: Vec<JsVirtualChunkSpec>,
        validate_containers: bool,
    ) -> napi::Result<Option<Vec<Vec<u32>>>> {
        let vrefs: Vec<(ChunkIndices, VirtualChunkRef)> = chunks
            .into_iter()
            .map(|spec| {
                let checksum = create_checksum(spec.etag_checksum, spec.last_modified);
                let index = ChunkIndices(spec.index);
                let location = VirtualChunkLocation::from_url(spec.location.as_str())
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
                let vref = VirtualChunkRef {
                    offset: spec.offset as u64,
                    length: spec.length as u64,
                    location,
                    checksum,
                };
                Ok::<_, napi::Error>((index, vref))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let array_path = if !array_path.starts_with('/') {
            format!("/{array_path}")
        } else {
            array_path
        };

        let path = Path::try_from(array_path)
            .map_err(|e| napi::Error::from_reason(format!("Invalid array path: {e}")))?;

        let res = self
            .0
            .set_virtual_refs(&path, validate_containers, vrefs)
            .await
            .map_napi_err()?;

        match res {
            SetVirtualRefsResult::Done => Ok(None),
            SetVirtualRefsResult::FailedRefs(vec) => {
                Ok(Some(vec.into_iter().map(|ci| ci.0).collect()))
            }
        }
    }
}
