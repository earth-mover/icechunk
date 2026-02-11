use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use icechunk::format::ByteRange;
use icechunk::store::{Store, StoreErrorKind};
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

use crate::errors::IntoNapiResult;

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

#[napi]
impl JsStore {
    #[napi]
    pub async fn get(&self, key: String) -> napi::Result<Option<Buffer>> {
        match self.0.get(&key, &ByteRange::ALL).await {
            Ok(bytes) => Ok(Some(bytes.to_vec().into())),
            Err(e) if matches!(e.kind, StoreErrorKind::NotFound(_)) => Ok(None),
            Err(e) => Err(napi::Error::from_reason(e.to_string())),
        }
    }

    #[napi]
    pub async fn get_range(
        &self,
        key: String,
        offset: i64,
        length: Option<i64>,
    ) -> napi::Result<Option<Buffer>> {
        let byte_range = match length {
            Some(len) => ByteRange::from_offset_with_length(offset as u64, len as u64),
            None => ByteRange::from_offset(offset as u64),
        };
        match self.0.get(&key, &byte_range).await {
            Ok(bytes) => Ok(Some(bytes.to_vec().into())),
            Err(e) if matches!(e.kind, StoreErrorKind::NotFound(_)) => Ok(None),
            Err(e) => Err(napi::Error::from_reason(e.to_string())),
        }
    }

    #[napi]
    pub async fn set(&self, key: String, value: Buffer) -> napi::Result<()> {
        let bytes = Bytes::from(value.to_vec());
        self.0.set(&key, bytes).await.map_napi_err()
    }

    #[napi]
    pub async fn exists(&self, key: String) -> napi::Result<bool> {
        self.0.exists(&key).await.map_napi_err()
    }

    #[napi]
    pub async fn delete(&self, key: String) -> napi::Result<()> {
        self.0.delete(&key).await.map_napi_err()
    }

    #[napi]
    pub async fn list(&self) -> napi::Result<Vec<String>> {
        let stream = self.0.list().await.map_napi_err()?;
        let keys: Vec<String> = stream.try_collect().await.map_napi_err()?;
        Ok(keys)
    }

    #[napi]
    pub async fn list_prefix(&self, prefix: String) -> napi::Result<Vec<String>> {
        let stream = self.0.list_prefix(&prefix).await.map_napi_err()?;
        let keys: Vec<String> = stream.try_collect().await.map_napi_err()?;
        Ok(keys)
    }

    #[napi]
    pub async fn list_dir(&self, prefix: String) -> napi::Result<Vec<String>> {
        let stream = self.0.list_dir(&prefix).await.map_napi_err()?;
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
        let location = VirtualChunkLocation::from_absolute_path(location.as_str())
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let checksum = create_checksum(etag_checksum, last_modified);
        let virtual_ref = VirtualChunkRef {
            location,
            offset: offset as u64,
            length: length as u64,
            checksum,
        };
        self.0.set_virtual_ref(&key, virtual_ref, validate_container).await.map_napi_err()
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
                let location =
                    VirtualChunkLocation::from_absolute_path(spec.location.as_str())
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
