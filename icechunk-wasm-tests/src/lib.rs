//! Support code for running icechunk on `wasm32-unknown-unknown`.
//!
//! [`JsYieldStorage`] is the shape a browser `Storage` takes: every call awaits
//! a JS promise first, so its futures are not `Send`, exactly like a `fetch`
//! based backend. It delegates to in-memory storage so tests need no server.
#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use std::{fmt, ops::Range, pin::Pin, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, stream::BoxStream};
use icechunk::{
    Storage,
    storage::{
        DeleteObjectsResult, GetModifiedResult, ListInfo, RepositoryCreation, Settings,
        StorageContext, StorageError, StorageInfo, StorageResult, VersionInfo,
        VersionedUpdateResult,
    },
};
use serde::{Deserialize, Serialize};

/// Hand control back to the JS event loop, the way a `fetch` call would.
///
/// The returned future holds a `JsFuture`, which is not `Send`.
pub async fn yield_to_js() {
    let promise =
        js_sys::Promise::resolve(&wasm_bindgen_futures::wasm_bindgen::JsValue::NULL);
    #[expect(clippy::expect_used)]
    wasm_bindgen_futures::JsFuture::from(promise).await.expect("resolved promise");
}

/// A `Storage` whose futures are not `Send`, backed by another storage.
#[derive(Debug, Serialize, Deserialize)]
pub struct JsYieldStorage {
    #[serde(skip, default = "unreachable_backend")]
    backend: Option<Arc<dyn Storage + Send + Sync>>,
}

fn unreachable_backend() -> Option<Arc<dyn Storage + Send + Sync>> {
    None
}

impl JsYieldStorage {
    pub fn new(backend: Arc<dyn Storage + Send + Sync>) -> Self {
        Self { backend: Some(backend) }
    }

    #[expect(clippy::expect_used)]
    fn backend(&self) -> &(dyn Storage + Send + Sync) {
        self.backend.as_deref().expect("JsYieldStorage is not deserializable")
    }
}

impl fmt::Display for JsYieldStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JsYieldStorage({})", self.backend())
    }
}

impl icechunk_storage::sealed::Sealed for JsYieldStorage {}

// Unconditionally `?Send`: this crate only builds for the browser. A plain
// `#[async_trait]` here would not compile, which is the point.
#[async_trait(?Send)]
#[typetag::serde]
impl Storage for JsYieldStorage {
    fn storage_info(&self) -> StorageInfo {
        StorageInfo { backend_type: "JsYield", fields: vec![] }
    }

    async fn default_settings(&self) -> StorageResult<Settings> {
        yield_to_js().await;
        self.backend().default_settings().await
    }

    async fn can_write(&self) -> StorageResult<bool> {
        yield_to_js().await;
        self.backend().can_write().await
    }

    async fn can_create_repository(&self) -> StorageResult<RepositoryCreation> {
        yield_to_js().await;
        self.backend().can_create_repository().await
    }

    async fn get_object_range(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<(
        Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>,
        VersionInfo,
    )> {
        yield_to_js().await;
        self.backend().get_object_range(ctx, path, range).await
    }

    async fn put_object(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<VersionedUpdateResult> {
        yield_to_js().await;
        self.backend()
            .put_object(ctx, path, bytes, content_type, metadata, previous_version)
            .await
    }

    async fn copy_object(
        &self,
        ctx: &StorageContext<'_>,
        from: &str,
        to: &str,
        content_type: Option<&str>,
        version: &VersionInfo,
    ) -> StorageResult<VersionedUpdateResult> {
        yield_to_js().await;
        self.backend().copy_object(ctx, from, to, content_type, version).await
    }

    async fn list_objects<'a>(
        &'a self,
        ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        yield_to_js().await;
        self.backend().list_objects(ctx, prefix).await
    }

    async fn delete_batch(
        &self,
        ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        yield_to_js().await;
        self.backend().delete_batch(ctx, prefix, batch).await
    }

    async fn get_object_last_modified(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        yield_to_js().await;
        self.backend().get_object_last_modified(ctx, path).await
    }

    async fn get_object_conditional(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        yield_to_js().await;
        self.backend().get_object_conditional(ctx, path, previous_version).await
    }
}
