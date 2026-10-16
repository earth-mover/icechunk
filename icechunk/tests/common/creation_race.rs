//! Two creators of one repository, sequenced by a storage wrapper: both pass
//! the clean-root check, creator A writes the initial snapshot first, and
//! creator B builds its own initial snapshot only after that, then writes it.

use std::{
    error::Error,
    fmt,
    ops::Range,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, TryStreamExt as _, stream::BoxStream};
use icechunk::{
    Repository, Storage,
    format::{REPO_INFO_FILE_PATH, SNAPSHOTS_FILE_PATH, snapshot::Snapshot},
    repository::VersionInfo as RepoVersionInfo,
    storage::{
        DeleteObjectsResult, GcPhase, GetModifiedResult, ListInfo, PutCondition,
        RepositoryCreation, Settings, StorageContext, StorageError, StorageInfo,
        StorageResult, StorageWriterFeatures, VersionInfo, VersionedUpdateResult,
        WriteHint,
    },
};
use icechunk_storage::sealed;
use pretty_assertions::assert_eq;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy)]
pub(crate) enum RepoInfoWinner {
    A,
    B,
}

/// Raised by a creator's storage when a step returns.
#[derive(Debug, Clone, Default)]
struct Signals {
    clean_checked: Arc<Notify>,
    snapshot_written: Arc<Notify>,
    repo_info_written: Arc<Notify>,
}

/// Forwards to `backend`. Waits for `resume_after_clean_check` once the
/// clean-root check returns and for `resume_before_repo_info` before writing
/// the repo info, and raises `signals`.
#[derive(Debug, Serialize, Deserialize)]
struct CreatorStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    #[serde(skip)]
    signals: Signals,
    #[serde(skip)]
    resume_after_clean_check: Option<Arc<Notify>>,
    #[serde(skip)]
    resume_before_repo_info: Option<Arc<Notify>>,
    /// The bytes this creator tried to write as the initial snapshot.
    #[serde(skip)]
    snapshot_attempt: Mutex<Option<Bytes>>,
}

impl CreatorStorage {
    fn snapshot_attempt(&self) -> Option<Bytes> {
        self.snapshot_attempt.lock().unwrap().clone()
    }

    fn is_initial_snapshot(path: &str) -> bool {
        path == format!("{SNAPSHOTS_FILE_PATH}/{}", Snapshot::INITIAL_SNAPSHOT_ID)
    }
}

impl fmt::Display for CreatorStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CreatorStorage({})", self.backend)
    }
}

impl sealed::Sealed for CreatorStorage {}

#[async_trait]
#[typetag::serde]
impl Storage for CreatorStorage {
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
        self.backend.writer_features().await
    }

    async fn gc_hook(
        &self,
        ctx: &StorageContext<'_>,
        phase: GcPhase<'_>,
    ) -> StorageResult<()> {
        self.backend.gc_hook(ctx, phase).await
    }

    async fn create_location_if_needed(&self) -> StorageResult<()> {
        self.backend.create_location_if_needed().await
    }

    async fn root_is_clean(&self, ctx: &StorageContext<'_>) -> StorageResult<bool> {
        let res = self.backend.root_is_clean(ctx).await;
        self.signals.clean_checked.notify_one();
        if let Some(resume) = &self.resume_after_clean_check {
            resume.notified().await;
        }
        res
    }

    async fn can_create_repository(&self) -> StorageResult<RepositoryCreation> {
        self.backend.can_create_repository().await
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
        let initial_snapshot = Self::is_initial_snapshot(path);
        if initial_snapshot {
            *self.snapshot_attempt.lock().unwrap() = Some(bytes.clone());
        }
        let res =
            self.backend.put_object(ctx, path, bytes, content_type, metadata, hint).await;
        if initial_snapshot {
            self.signals.snapshot_written.notify_one();
        }
        res
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
        let initial_snapshot = Self::is_initial_snapshot(path);
        let repo_info = path == REPO_INFO_FILE_PATH;
        if initial_snapshot {
            *self.snapshot_attempt.lock().unwrap() = Some(bytes.clone());
        }
        if repo_info && let Some(resume) = &self.resume_before_repo_info {
            resume.notified().await;
        }
        let res = self
            .backend
            .put_object_conditional(ctx, path, bytes, content_type, metadata, condition)
            .await;
        if initial_snapshot {
            self.signals.snapshot_written.notify_one();
        }
        if repo_info {
            self.signals.repo_info_written.notify_one();
        }
        res
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
        self.backend.list_objects(ctx, prefix).await
    }

    async fn list_objects_with_id_prefixes<'a>(
        &'a self,
        ctx: &StorageContext<'_>,
        prefix: &str,
        id_prefixes: &[String],
    ) -> StorageResult<BoxStream<'a, StorageResult<ListInfo<String>>>> {
        self.backend.list_objects_with_id_prefixes(ctx, prefix, id_prefixes).await
    }

    fn lists_id_prefixes_natively(&self) -> bool {
        self.backend.lists_id_prefixes_natively()
    }

    fn supports_ic1_repos(&self) -> bool {
        self.backend.supports_ic1_repos()
    }

    async fn delete_batch(
        &self,
        ctx: &StorageContext<'_>,
        prefix: &str,
        batch: Vec<(String, u64)>,
    ) -> StorageResult<DeleteObjectsResult> {
        self.backend.delete_batch(ctx, prefix, batch).await
    }

    async fn sweep_leftovers(
        &self,
        ctx: &StorageContext<'_>,
        older_than: DateTime<Utc>,
        dry_run: bool,
    ) -> StorageResult<DeleteObjectsResult> {
        self.backend.sweep_leftovers(ctx, older_than, dry_run).await
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
        self.backend.get_object_conditional(ctx, path, previous_version).await
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
}

/// Creates one repository through `a` and `b` at once, in the interleaving
/// the module describes, with `winner`'s repo info landing first. Checks that
/// the stored repo info's initial `flushed_at` equals the stored snapshot
/// file's.
pub(crate) async fn race_creators(
    a: Arc<dyn Storage + Send + Sync>,
    b: Arc<dyn Storage + Send + Sync>,
    winner: RepoInfoWinner,
) -> Result<(), Box<dyn Error>> {
    let (a_signals, b_signals) = (Signals::default(), Signals::default());
    let (a_waits_for_repo_info, b_waits_for_repo_info) = match winner {
        RepoInfoWinner::A => (None, Some(Arc::clone(&a_signals.repo_info_written))),
        RepoInfoWinner::B => (Some(Arc::clone(&b_signals.repo_info_written)), None),
    };
    let creator_a = Arc::new(CreatorStorage {
        backend: Arc::clone(&a),
        resume_after_clean_check: Some(Arc::clone(&b_signals.clean_checked)),
        resume_before_repo_info: a_waits_for_repo_info,
        signals: a_signals.clone(),
        snapshot_attempt: Mutex::new(None),
    });
    let creator_b = Arc::new(CreatorStorage {
        backend: b,
        resume_after_clean_check: Some(Arc::clone(&a_signals.snapshot_written)),
        resume_before_repo_info: b_waits_for_repo_info,
        signals: b_signals,
        snapshot_attempt: Mutex::new(None),
    });

    let (res_a, res_b) = tokio::time::timeout(Duration::from_secs(60), async {
        tokio::join!(
            Repository::create(Arc::clone(&creator_a) as _).execute(),
            Repository::create(Arc::clone(&creator_b) as _).execute(),
        )
    })
    .await?;
    let (won, lost) = match winner {
        RepoInfoWinner::A => (res_a, res_b),
        RepoInfoWinner::B => (res_b, res_a),
    };
    won?;
    assert!(lost.is_err(), "both creators succeeded");
    assert!(
        creator_a.snapshot_attempt() != creator_b.snapshot_attempt(),
        "the creators built identical initial snapshots, the race shows nothing"
    );

    let repo = Repository::open(a).execute().await?;
    let history: Vec<_> = repo
        .ancestry(&RepoVersionInfo::BranchTipRef("main".to_string()))
        .await?
        .try_collect()
        .await?;
    let [initial] = history.as_slice() else {
        panic!("expected only the initial snapshot, got {history:?}");
    };
    let file =
        repo.asset_manager().fetch_snapshot(&Snapshot::INITIAL_SNAPSHOT_ID).await?;
    assert_eq!(initial.flushed_at, file.flushed_at()?);
    Ok(())
}
