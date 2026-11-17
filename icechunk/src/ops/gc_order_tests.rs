//! The order of GC's change, the sweep and the deletes, through real
//! repositories on in-memory storage and a wrapper that records those calls
//! and can report the filesystem layout's writer feature.
//!
//! Compiled only for tests, and not with the `shuttle` feature.

use std::{
    num::{NonZeroU16, NonZeroUsize},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use futures::{StreamExt as _, future::BoxFuture, stream::BoxStream};
use icechunk_macros::tokio_test;

use crate::{
    Repository, Storage,
    format::{
        CHUNKS_FILE_PATH, ChunkId, ChunkIndices, Path, REPO_INFO_FILE_PATH,
        repo_info::{RepoAvailability, RepoStatus, UpdateType},
        snapshot::ArrayShape,
        writer_features::WriterFeature,
    },
    ops::{
        GCError,
        gc::{Action, GCConfig, GCSummary, garbage_collect},
    },
    repository::RepositoryErrorKind,
    storage::{
        DeleteObjectsResult, ListInfo, PutCondition, StorageContext, StorageResult,
        StorageWriterFeatures, VersionInfo, VersionedUpdateResult, WriteHint,
        intercepting::{Intercept, InterceptingStorage},
        new_in_memory_storage,
    },
};

type TestResult<A = ()> = Result<A, Box<dyn std::error::Error>>;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    /// A repo-info write that landed, with the version it wrote.
    RepoInfoWritten(VersionInfo),
    Sweep,
    /// A `delete_batch` under this prefix.
    Delete(String),
}

/// Runs once, the first time the storage lists anything.
#[derive(Default)]
struct OnFirstListing(tokio::sync::Mutex<Option<BoxFuture<'static, ()>>>);

impl std::fmt::Debug for OnFirstListing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OnFirstListing")
    }
}

/// Records [`Event`]s of the storage [`GcTestStorage::wrapped`] makes.
/// Implements and requires `filesystem-layout-v1` when `layout`.
#[derive(Debug)]
struct GcTestStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    layout: bool,
    events: Mutex<Vec<Event>>,
    on_first_listing: OnFirstListing,
}

impl GcTestStorage {
    fn new(backend: Arc<dyn Storage + Send + Sync>, layout: bool) -> Arc<Self> {
        Arc::new(Self {
            backend,
            layout,
            events: Mutex::new(Vec::new()),
            on_first_listing: OnFirstListing::default(),
        })
    }

    /// A storage over `backend` intercepted by `self`.
    fn wrapped(self: &Arc<Self>) -> Arc<dyn Storage + Send + Sync> {
        InterceptingStorage::new(Arc::clone(&self.backend), Arc::clone(self) as _)
    }

    fn record(&self, event: Event) {
        self.events.lock().unwrap_or_else(|p| p.into_inner()).push(event);
    }

    /// The events since the last call, consecutive deletes under one prefix
    /// merged into one.
    fn take_events(&self) -> Vec<Event> {
        let mut events =
            std::mem::take(&mut *self.events.lock().unwrap_or_else(|p| p.into_inner()));
        events.dedup();
        events
    }

    async fn listing(&self) {
        let action = self.on_first_listing.0.lock().await.take();
        if let Some(action) = action {
            action.await;
        }
    }
}

#[async_trait::async_trait]
impl Intercept for GcTestStorage {
    async fn writer_features(
        &self,
        _backend: &(dyn Storage + Send + Sync),
    ) -> StorageResult<StorageWriterFeatures> {
        Ok(if self.layout {
            StorageWriterFeatures::from([WriterFeature::FilesystemLayoutV1])
        } else {
            StorageWriterFeatures::new()
        })
    }

    async fn put_object_conditional(
        &self,
        backend: &(dyn Storage + Send + Sync),
        ctx: &StorageContext<'_>,
        path: &str,
        bytes: Bytes,
        content_type: Option<&str>,
        metadata: Vec<(String, String)>,
        condition: PutCondition<'_>,
    ) -> StorageResult<VersionedUpdateResult> {
        let res = backend
            .put_object_conditional(ctx, path, bytes, content_type, metadata, condition)
            .await?;
        match res {
            VersionedUpdateResult::Updated { new_version }
                if path == REPO_INFO_FILE_PATH =>
            {
                self.record(Event::RepoInfoWritten(new_version.clone()));
                Ok(VersionedUpdateResult::Updated { new_version })
            }
            res => Ok(res),
        }
    }

    async fn list_objects<'a>(
        &self,
        backend: &'a (dyn Storage + Send + Sync),
        ctx: &StorageContext<'_>,
        prefix: &str,
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.listing().await;
        backend.list_objects(ctx, prefix).await
    }

    async fn list_objects_with_id_prefixes<'a>(
        &self,
        backend: &'a (dyn Storage + Send + Sync),
        ctx: &StorageContext<'_>,
        prefix: &str,
        id_prefixes: &[String],
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.listing().await;
        backend.list_objects_with_id_prefixes(ctx, prefix, id_prefixes).await
    }

    async fn delete_batch(
        &self,
        backend: &(dyn Storage + Send + Sync),
        ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.record(Event::Delete(prefix.to_string()));
        backend.delete_batch(ctx, prefix, batch).await
    }

    async fn sweep_leftovers(
        &self,
        backend: &(dyn Storage + Send + Sync),
        ctx: &StorageContext<'_>,
        older_than: DateTime<Utc>,
        dry_run: bool,
    ) -> StorageResult<DeleteObjectsResult> {
        self.record(Event::Sweep);
        backend.sweep_leftovers(ctx, older_than, dry_run).await
    }
}

/// A repository whose `main` keeps one commit with an array and four chunks,
/// after a reset that left a second commit, its manifest and four chunks
/// unreachable.
async fn repo_with_garbage(storage: Arc<GcTestStorage>) -> TestResult<Repository> {
    let repo = Repository::create(storage.wrapped()).execute().await?;
    let array: Path = "/arr".try_into()?;
    let mut keep = None;
    for commit in 0..2u8 {
        let mut session = repo.writable_session("main").await?;
        if commit == 0 {
            session
                .add_array(
                    array.clone(),
                    ArrayShape::new([(4, 4)]).ok_or("bad shape")?,
                    None,
                    Bytes::from_static(br#"{"zarr_format":3}"#),
                )
                .await?;
        }
        for i in 0..4u32 {
            // above the inline threshold, so each chunk is its own object
            let payload = session.get_chunk_writer(&array, &ChunkIndices(vec![i]))?(
                Bytes::from(vec![commit; 1024]),
            )
            .await?;
            session
                .set_chunk_ref(array.clone(), ChunkIndices(vec![i]), Some(payload))
                .await?;
        }
        let snap = session.commit("commit").execute().await?;
        keep.get_or_insert(snap);
    }
    repo.reset_branch("main", &keep.ok_or("no commit")?, None).await?;
    Ok(repo)
}

fn config(chunks: bool, metadata: bool, dry_run: bool) -> GCConfig {
    // in the future: everything unreachable is old enough
    let cutoff = Utc::now() + Duration::hours(1);
    let action = |deletes| {
        if deletes { Action::DeleteIfCreatedBefore(cutoff) } else { Action::Keep }
    };
    GCConfig::new(
        Default::default(),
        action(chunks),
        action(metadata),
        action(metadata),
        action(metadata),
        action(metadata),
        Action::DeleteIfCreatedBefore(cutoff),
        NonZeroU16::new(10).unwrap_or(NonZeroU16::MIN),
        NonZeroUsize::new(1 << 30).unwrap_or(NonZeroUsize::MIN),
        NonZeroUsize::new(1 << 30).unwrap_or(NonZeroUsize::MIN),
        NonZeroU16::new(10).unwrap_or(NonZeroU16::MIN),
        NonZeroU16::new(10).unwrap_or(NonZeroU16::MIN),
        NonZeroU16::new(10).unwrap_or(NonZeroU16::MIN),
        None,
        dry_run,
    )
}

fn clean_all(dry_run: bool) -> GCConfig {
    config(true, true, dry_run)
}

async fn gc(repo: &Repository, config: &GCConfig) -> Result<GCSummary, GCError> {
    garbage_collect(Arc::clone(repo.asset_manager()), config, None, 100).await
}

async fn latest_update(repo: &Repository) -> TestResult<UpdateType> {
    let (info, _) = repo.asset_manager().fetch_repo_info().await?;
    let (update, _, _) = info.latest_updates()?.next().ok_or("empty ops log")??;
    Ok(update)
}

async fn object_count(repo: &Repository) -> TestResult<usize> {
    let am = repo.asset_manager();
    Ok(am.list_chunks().await?.count().await
        + am.list_manifests().await?.count().await
        + am.list_snapshots().await?.count().await
        + am.list_transaction_logs().await?.count().await)
}

fn deletes(prefixes: &[&str]) -> Vec<Event> {
    prefixes.iter().map(|p| Event::Delete(p.to_string())).collect()
}

/// The version of the one repo-info write among `events`.
#[expect(clippy::panic, reason = "test helper")]
fn only_change(events: &[Event]) -> VersionInfo {
    let written: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            Event::RepoInfoWritten(v) => Some(v.clone()),
            _ => None,
        })
        .collect();
    match &written[..] {
        [v] => v.clone(),
        _ => panic!("one change expected: {events:?}"),
    }
}

const ALL_PREFIXES: [&str; 4] = ["snapshots", "transactions", "manifests", "chunks"];

/// In both layouts: the change, the sweep, then the deletes.
#[tokio_test]
async fn gc_changes_then_sweeps_then_deletes() -> TestResult {
    for layout in [true, false] {
        let storage = GcTestStorage::new(new_in_memory_storage().await?, layout);
        let repo = repo_with_garbage(Arc::clone(&storage)).await?;
        storage.take_events();

        let summary = gc(&repo, &clean_all(false)).await?;
        assert_eq!(summary.snapshots_deleted, 1, "layout={layout}");
        assert_eq!(summary.chunks_deleted, 4, "layout={layout}");

        let events = storage.take_events();
        let mut expected =
            vec![Event::RepoInfoWritten(only_change(&events)), Event::Sweep];
        expected.extend(deletes(&ALL_PREFIXES));
        assert_eq!(events, expected, "layout={layout}");
        assert_eq!(latest_update(&repo).await?, UpdateType::GCRanUpdate);
    }
    Ok(())
}

/// In both layouts, GC that keeps every snapshot makes no change, whether it
/// deletes chunks and leftovers or only leftovers.
#[tokio_test]
async fn gc_that_keeps_snapshots_makes_no_change() -> TestResult {
    for layout in [true, false] {
        for chunks in [true, false] {
            let storage = GcTestStorage::new(new_in_memory_storage().await?, layout);
            let repo = repo_with_garbage(Arc::clone(&storage)).await?;
            // the kept snapshots keep their chunks: give GC one to delete
            storage
                .backend
                .put_object(
                    &repo.storage_context(),
                    &format!("{CHUNKS_FILE_PATH}/{}", ChunkId::random()),
                    Bytes::from_static(b"x"),
                    None,
                    Vec::new(),
                    WriteHint::default(),
                )
                .await?;
            let (_, version_before) = repo.asset_manager().fetch_repo_info().await?;
            storage.take_events();

            let summary = gc(&repo, &config(chunks, false, false)).await?;
            let case = format!("layout={layout} chunks={chunks}");
            assert_eq!(summary.chunks_deleted, u64::from(chunks), "{case}");
            let mut expected = vec![Event::Sweep];
            if chunks {
                expected.extend(deletes(&["chunks"]));
            }
            assert_eq!(storage.take_events(), expected, "{case}");
            let (_, version_after) = repo.asset_manager().fetch_repo_info().await?;
            assert_eq!(version_after, version_before, "{case}");
        }
    }
    Ok(())
}

fn read_only() -> RepoStatus {
    RepoStatus {
        availability: RepoAvailability::ReadOnly,
        set_at: Utc::now(),
        limited_availability_reason: None,
    }
}

fn is_not_online(err: &GCError) -> bool {
    matches!(
        err,
        GCError::Repository(e) if matches!(e.kind, RepositoryErrorKind::ReadonlyRepository(_))
    )
}

/// A `ReadOnly` status refuses GC, whether it was set before GC started or
/// landed between GC's checks and its change; nothing is changed or deleted.
#[tokio_test]
async fn read_only_status_refuses_gc() -> TestResult {
    for layout in [true, false] {
        let storage = GcTestStorage::new(new_in_memory_storage().await?, layout);
        let repo = repo_with_garbage(Arc::clone(&storage)).await?;
        let before = object_count(&repo).await?;
        repo.set_status(&read_only()).await?;
        storage.take_events();
        let err = gc(&repo, &clean_all(false)).await.err().ok_or("GC succeeded")?;
        assert!(is_not_online(&err), "{err}");
        assert_eq!(storage.take_events(), vec![], "layout={layout}");
        assert_eq!(object_count(&repo).await?, before);

        let storage = GcTestStorage::new(new_in_memory_storage().await?, layout);
        let repo = repo_with_garbage(Arc::clone(&storage)).await?;
        let before = object_count(&repo).await?;
        let other = Repository::open(
            GcTestStorage::new(Arc::clone(&storage.backend), layout).wrapped(),
        )
        .execute()
        .await?;
        *storage.on_first_listing.0.lock().await = Some(Box::pin(async move {
            other.set_status(&read_only()).await.expect("set_status failed");
        }));
        storage.take_events();
        let err = gc(&repo, &clean_all(false)).await.err().ok_or("GC succeeded")?;
        assert!(is_not_online(&err), "{err}");
        assert_eq!(storage.take_events(), vec![], "layout={layout}");
        assert_eq!(object_count(&repo).await?, before);
        assert_eq!(repo.get_status().await?.availability, RepoAvailability::ReadOnly);
    }
    Ok(())
}

/// A dry run changes nothing and deletes nothing.
#[tokio_test]
async fn dry_runs_change_nothing() -> TestResult {
    for layout in [true, false] {
        let storage = GcTestStorage::new(new_in_memory_storage().await?, layout);
        let repo = repo_with_garbage(Arc::clone(&storage)).await?;
        let before = object_count(&repo).await?;
        let (_, version_before) = repo.asset_manager().fetch_repo_info().await?;
        storage.take_events();

        let summary = gc(&repo, &clean_all(true)).await?;
        assert_eq!(summary.chunks_deleted, 4, "layout={layout}");
        assert_eq!(storage.take_events(), vec![Event::Sweep], "layout={layout}");
        assert_eq!(object_count(&repo).await?, before);
        let (_, version_after) = repo.asset_manager().fetch_repo_info().await?;
        assert_eq!(version_after, version_before);
    }
    Ok(())
}
