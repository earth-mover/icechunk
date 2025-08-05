use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use core::fmt;
use futures::{StreamExt, stream::BoxStream};
use std::ops::Range;
use tokio::io::AsyncRead;

use crate::{
    format::{ChunkId, ChunkOffset, ManifestId, SnapshotId},
    private,
};

use super::{
    DeleteObjectsResult, FetchConfigResult, GetRefResult, ListInfo, Reader, Settings,
    StorageResult, UpdateConfigResult, VersionInfo, WriteRefResult,
};

/// Fetch and write the parquet files that represent the repository in object store
///
/// Different implementation can cache the files differently, or not at all.
/// Implementations are free to assume files are never overwritten.
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Storage: fmt::Debug + fmt::Display + private::Sealed + Sync + Send {
    fn default_settings(&self) -> Settings {
        Default::default()
    }

    fn can_write(&self) -> bool;

    async fn fetch_config(&self, settings: &Settings)
    -> StorageResult<FetchConfigResult>;
    async fn update_config(
        &self,
        settings: &Settings,
        config: Bytes,
        previous_version: &VersionInfo,
    ) -> StorageResult<UpdateConfigResult>;
    async fn fetch_snapshot(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;
    /// Returns whatever reader is more efficient.
    ///
    /// For example, if processed with multiple requests, it will return a synchronous `Buf`
    /// instance pointing the different parts. If it was executed in a single request, it's more
    /// efficient to return the network `AsyncRead` directly
    async fn fetch_manifest_known_size(
        &self,
        settings: &Settings,
        id: &ManifestId,
        size: u64,
    ) -> StorageResult<Reader>;
    async fn fetch_manifest_unknown_size(
        &self,
        settings: &Settings,
        id: &ManifestId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;
    async fn fetch_chunk(
        &self,
        settings: &Settings,
        id: &ChunkId,
        range: &Range<ChunkOffset>,
    ) -> StorageResult<Bytes>; // FIXME: format flags
    async fn fetch_transaction_log(
        &self,
        settings: &Settings,
        id: &SnapshotId,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;

    async fn write_snapshot(
        &self,
        settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()>;
    async fn write_manifest(
        &self,
        settings: &Settings,
        id: ManifestId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()>;
    async fn write_chunk(
        &self,
        settings: &Settings,
        id: ChunkId,
        bytes: Bytes,
    ) -> StorageResult<()>;
    async fn write_transaction_log(
        &self,
        settings: &Settings,
        id: SnapshotId,
        metadata: Vec<(String, String)>,
        bytes: Bytes,
    ) -> StorageResult<()>;

    async fn get_ref(
        &self,
        settings: &Settings,
        ref_key: &str,
    ) -> StorageResult<GetRefResult>;
    async fn ref_names(&self, settings: &Settings) -> StorageResult<Vec<String>>;
    async fn write_ref(
        &self,
        settings: &Settings,
        ref_key: &str,
        bytes: Bytes,
        previous_version: &VersionInfo,
    ) -> StorageResult<WriteRefResult>;

    async fn list_objects<'a>(
        &'a self,
        settings: &Settings,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>>;

    async fn delete_batch(
        &self,
        settings: &Settings,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult>;

    /// Delete a stream of objects, by their id string representations
    /// Input stream includes sizes to get as result the total number of bytes deleted
    /// Delete a stream of objects, by their id string representations
    /// Input stream includes sizes to get as result the total number of bytes deleted
    async fn delete_objects(
        &self,
        settings: &Settings,
        prefix: &str,
        ids: BoxStream<'_, (String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        use futures::StreamExt;
        use std::sync::{Arc, Mutex};
        use tracing::warn;

        let res = Arc::new(Mutex::new(DeleteObjectsResult::default()));
        ids.chunks(1_000)
            // FIXME: configurable concurrency
            .for_each_concurrent(10, |batch| {
                let res = Arc::clone(&res);
                async move {
                    let new_deletes = self
                        .delete_batch(settings, prefix, batch)
                        .await
                        .unwrap_or_else(|_| {
                            // FIXME: handle error instead of skipping
                            warn!("ignoring error in Storage::delete_batch");
                            Default::default()
                        });
                    #[allow(clippy::expect_used)]
                    res.lock().expect("Bug in delete objects").merge(&new_deletes);
                }
            })
            .await;
        #[allow(clippy::expect_used)]
        let res = res.lock().expect("Bug in delete objects");
        Ok(res.clone())
    }

    async fn get_snapshot_last_modified(
        &self,
        settings: &Settings,
        snapshot: &SnapshotId,
    ) -> StorageResult<DateTime<Utc>>;

    async fn root_is_clean(&self) -> StorageResult<bool> {
        match self.list_objects(&Settings::default(), "").await?.next().await {
            None => Ok(true),
            Some(Ok(_)) => Ok(false),
            Some(Err(err)) => Err(err),
        }
    }

    async fn list_chunks(
        &self,
        settings: &Settings,
    ) -> StorageResult<BoxStream<StorageResult<ListInfo<ChunkId>>>> {
        use super::{CHUNK_PREFIX, translate_list_infos};
        Ok(translate_list_infos(self.list_objects(settings, CHUNK_PREFIX).await?))
    }

    async fn list_manifests(
        &self,
        settings: &Settings,
    ) -> StorageResult<BoxStream<StorageResult<ListInfo<ManifestId>>>> {
        use super::{MANIFEST_PREFIX, translate_list_infos};
        Ok(translate_list_infos(self.list_objects(settings, MANIFEST_PREFIX).await?))
    }

    async fn list_snapshots(
        &self,
        settings: &Settings,
    ) -> StorageResult<BoxStream<StorageResult<ListInfo<SnapshotId>>>> {
        use super::{SNAPSHOT_PREFIX, translate_list_infos};
        Ok(translate_list_infos(self.list_objects(settings, SNAPSHOT_PREFIX).await?))
    }

    async fn list_transaction_logs(
        &self,
        settings: &Settings,
    ) -> StorageResult<BoxStream<StorageResult<ListInfo<SnapshotId>>>> {
        use super::{TRANSACTION_PREFIX, translate_list_infos};
        Ok(translate_list_infos(self.list_objects(settings, TRANSACTION_PREFIX).await?))
    }

    async fn delete_chunks(
        &self,
        settings: &Settings,
        chunks: BoxStream<'_, (ChunkId, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        use super::CHUNK_PREFIX;
        use futures::StreamExt;
        self.delete_objects(
            settings,
            CHUNK_PREFIX,
            chunks.map(|(id, size)| (id.to_string(), size)).boxed(),
        )
        .await
    }

    async fn delete_manifests(
        &self,
        settings: &Settings,
        manifests: BoxStream<'_, (ManifestId, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        use super::MANIFEST_PREFIX;
        use futures::StreamExt;
        self.delete_objects(
            settings,
            MANIFEST_PREFIX,
            manifests.map(|(id, size)| (id.to_string(), size)).boxed(),
        )
        .await
    }

    async fn delete_snapshots(
        &self,
        settings: &Settings,
        snapshots: BoxStream<'_, (SnapshotId, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        use super::SNAPSHOT_PREFIX;
        use futures::StreamExt;
        self.delete_objects(
            settings,
            SNAPSHOT_PREFIX,
            snapshots.map(|(id, size)| (id.to_string(), size)).boxed(),
        )
        .await
    }

    async fn delete_transaction_logs(
        &self,
        settings: &Settings,
        transaction_logs: BoxStream<'_, (SnapshotId, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        use super::TRANSACTION_PREFIX;
        use futures::StreamExt;
        self.delete_objects(
            settings,
            TRANSACTION_PREFIX,
            transaction_logs.map(|(id, size)| (id.to_string(), size)).boxed(),
        )
        .await
    }

    async fn delete_refs(
        &self,
        settings: &Settings,
        refs: BoxStream<'_, String>,
    ) -> StorageResult<u64> {
        use super::REF_PREFIX;
        use futures::StreamExt;
        let refs = refs.map(|s| (s, 0)).boxed();
        Ok(self.delete_objects(settings, REF_PREFIX, refs).await?.deleted_objects)
    }

    async fn get_object_range_buf(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn bytes::Buf + Unpin + Send>>;

    async fn get_object_range_read(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Box<dyn AsyncRead + Unpin + Send>>;

    async fn get_object_concurrently_multiple(
        &self,
        settings: &Settings,
        key: &str,
        parts: Vec<Range<u64>>,
    ) -> StorageResult<Box<dyn bytes::Buf + Send + Unpin>> {
        use bytes::Buf;
        use futures::TryStreamExt;
        use futures::stream::FuturesOrdered;

        let results =
            parts
                .into_iter()
                .map(|range| async move {
                    self.get_object_range_buf(settings, key, &range).await
                })
                .collect::<FuturesOrdered<_>>();

        let init: Box<dyn Buf + Unpin + Send> = Box::new(&[][..]);
        let buf = results
            .try_fold(init, |prev, buf| async {
                let res: Box<dyn Buf + Unpin + Send> = Box::new(prev.chain(buf));
                Ok(res)
            })
            .await?;

        Ok(Box::new(buf))
    }

    async fn get_object_concurrently(
        &self,
        settings: &Settings,
        key: &str,
        range: &Range<u64>,
    ) -> StorageResult<Reader> {
        use super::split_in_multiple_requests;

        let parts = split_in_multiple_requests(
            range,
            settings.concurrency().ideal_concurrent_request_size().get(),
            settings.concurrency().max_concurrent_requests_for_object().get(),
        )
        .collect::<Vec<_>>();

        let res = match parts.len() {
            0 => Reader::Asynchronous(Box::new(tokio::io::empty())),
            1 => Reader::Asynchronous(
                self.get_object_range_read(settings, key, range).await?,
            ),
            _ => Reader::Synchronous(
                self.get_object_concurrently_multiple(settings, key, parts).await?,
            ),
        };
        Ok(res)
    }
}
