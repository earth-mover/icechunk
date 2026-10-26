//! A test storage that forwards every call to its backend, except the ones
//! its [`Intercept`] overrides.

use std::{fmt, ops::Range, pin::Pin, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, stream::BoxStream};
use serde::{Deserialize, Serialize};

use super::{
    DeleteObjectsResult, GcPhase, GetModifiedResult, ListInfo, PutCondition,
    RepositoryCreation, Settings, Storage, StorageContext, StorageError, StorageInfo,
    StorageResult, StorageWriterFeatures, VersionInfo, VersionedUpdateResult, WriteHint,
};

type Backend<'a> = &'a (dyn Storage + Send + Sync);

/// The calls tests intercept. Each one defaults to forwarding to `backend`.
#[async_trait]
pub(crate) trait Intercept: fmt::Debug + Send + Sync {
    async fn writer_features(
        &self,
        backend: Backend<'_>,
    ) -> StorageResult<StorageWriterFeatures> {
        backend.writer_features().await
    }

    async fn gc_hook(
        &self,
        backend: Backend<'_>,
        ctx: &StorageContext<'_>,
        phase: GcPhase<'_>,
    ) -> StorageResult<()> {
        backend.gc_hook(ctx, phase).await
    }

    #[expect(clippy::too_many_arguments)]
    async fn put_object_conditional(
        &self,
        backend: Backend<'_>,
        ctx: &StorageContext<'_>,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        condition: PutCondition<'_>,
    ) -> StorageResult<VersionedUpdateResult> {
        backend
            .put_object_conditional(ctx, path, bytes, content_type, metadata, condition)
            .await
    }

    async fn list_objects<'a>(
        &self,
        backend: Backend<'a>,
        ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        backend.list_objects(ctx, prefix).await
    }

    async fn list_objects_with_id_prefixes<'a>(
        &self,
        backend: Backend<'a>,
        ctx: &StorageContext<'_>,
        prefix: &str,
        id_prefixes: &[String],
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        backend.list_objects_with_id_prefixes(ctx, prefix, id_prefixes).await
    }

    fn lists_id_prefixes_natively(&self, backend: Backend<'_>) -> bool {
        backend.lists_id_prefixes_natively()
    }

    async fn delete_batch(
        &self,
        backend: Backend<'_>,
        ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        backend.delete_batch(ctx, prefix, batch).await
    }

    async fn sweep_leftovers(
        &self,
        backend: Backend<'_>,
        ctx: &StorageContext<'_>,
        older_than: DateTime<Utc>,
        dry_run: bool,
    ) -> StorageResult<DeleteObjectsResult> {
        backend.sweep_leftovers(ctx, older_than, dry_run).await
    }

    async fn get_object_conditional(
        &self,
        backend: Backend<'_>,
        ctx: &StorageContext<'_>,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        backend.get_object_conditional(ctx, path, previous_version).await
    }
}

/// Intercepts nothing: what a deserialized [`InterceptingStorage`] gets.
#[derive(Debug)]
struct ForwardAll;

impl Intercept for ForwardAll {}

fn forward_all() -> Arc<dyn Intercept> {
    Arc::new(ForwardAll)
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct InterceptingStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    #[serde(skip, default = "forward_all")]
    intercept: Arc<dyn Intercept>,
}

impl InterceptingStorage {
    pub(crate) fn new(
        backend: Arc<dyn Storage + Send + Sync>,
        intercept: Arc<dyn Intercept>,
    ) -> Arc<Self> {
        Arc::new(Self { backend, intercept })
    }
}

impl fmt::Display for InterceptingStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InterceptingStorage({})", self.backend)
    }
}

impl icechunk_storage::sealed::Sealed for InterceptingStorage {}

#[async_trait]
#[typetag::serde]
impl Storage for InterceptingStorage {
    fn storage_info(&self) -> StorageInfo {
        self.backend.storage_info()
    }

    async fn default_settings(&self) -> StorageResult<Settings> {
        self.backend.default_settings().await
    }

    async fn can_write(&self) -> StorageResult<bool> {
        self.backend.can_write().await
    }

    async fn writer_features(&self) -> StorageResult<StorageWriterFeatures> {
        self.intercept.writer_features(self.backend.as_ref()).await
    }

    async fn gc_hook(
        &self,
        ctx: &StorageContext<'_>,
        phase: GcPhase<'_>,
    ) -> StorageResult<()> {
        self.intercept.gc_hook(self.backend.as_ref(), ctx, phase).await
    }

    async fn can_create_repository(&self) -> StorageResult<RepositoryCreation> {
        self.backend.can_create_repository().await
    }

    async fn create_location_if_needed(&self) -> StorageResult<()> {
        self.backend.create_location_if_needed().await
    }

    async fn get_object_range(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        range: Option<&Range<u64>>,
    ) -> StorageResult<Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>>
    {
        self.backend.get_object_range(ctx, path, range).await
    }

    async fn put_object(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        hint: WriteHint,
    ) -> StorageResult<()> {
        self.backend.put_object(ctx, path, bytes, content_type, metadata, hint).await
    }

    async fn put_object_conditional(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        condition: PutCondition<'_>,
    ) -> StorageResult<VersionedUpdateResult> {
        self.intercept
            .put_object_conditional(
                self.backend.as_ref(),
                ctx,
                path,
                bytes,
                content_type,
                metadata,
                condition,
            )
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
        self.backend.copy_object(ctx, from, to, content_type, version).await
    }

    async fn list_objects<'a>(
        &'a self,
        ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.intercept.list_objects(self.backend.as_ref(), ctx, prefix).await
    }

    async fn list_objects_with_id_prefixes<'a>(
        &'a self,
        ctx: &StorageContext<'_>,
        prefix: &str,
        id_prefixes: &[String],
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.intercept
            .list_objects_with_id_prefixes(
                self.backend.as_ref(),
                ctx,
                prefix,
                id_prefixes,
            )
            .await
    }

    fn lists_id_prefixes_natively(&self) -> bool {
        self.intercept.lists_id_prefixes_natively(self.backend.as_ref())
    }

    async fn delete_batch(
        &self,
        ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.intercept.delete_batch(self.backend.as_ref(), ctx, prefix, batch).await
    }

    async fn sweep_leftovers(
        &self,
        ctx: &StorageContext<'_>,
        older_than: DateTime<Utc>,
        dry_run: bool,
    ) -> StorageResult<DeleteObjectsResult> {
        self.intercept
            .sweep_leftovers(self.backend.as_ref(), ctx, older_than, dry_run)
            .await
    }

    async fn get_object_last_modified(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
    ) -> StorageResult<DateTime<Utc>> {
        self.backend.get_object_last_modified(ctx, path).await
    }

    async fn get_object_conditional(
        &self,
        ctx: &StorageContext<'_>,
        path: &str,
        previous_version: Option<&VersionInfo>,
    ) -> StorageResult<GetModifiedResult> {
        self.intercept
            .get_object_conditional(self.backend.as_ref(), ctx, path, previous_version)
            .await
    }

    async fn root_is_clean(&self, ctx: &StorageContext<'_>) -> StorageResult<bool> {
        self.backend.root_is_clean(ctx).await
    }
}
