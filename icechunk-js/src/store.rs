use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use icechunk::format::ByteRange;
use icechunk::store::{Store, StoreErrorKind};
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

use crate::errors::IntoNapiResult;

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
