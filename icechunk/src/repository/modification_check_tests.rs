//! The checks before every modification (writer features, required features,
//! shape, status) and the durable-but-forked change, through real
//! repositories on in-memory storage and a wrapper that changes what the
//! storage implements and how its repo-info writes end.
//!
//! Compiled only for tests.

use std::{
    collections::BTreeSet,
    num::{NonZeroU16, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use bytes::Bytes;
use chrono::Utc;
use icechunk_macros::tokio_test;

use crate::{
    Repository, Storage,
    asset_manager::write_repo_info,
    format::{
        IcechunkFormatErrorKind, IcechunkResult, Path, REPO_INFO_FILE_PATH, SnapshotId,
        flatbuffers::generated,
        format_constants::SpecVersionBin,
        repo_info::{
            RepoAvailability, RepoInfo, RepoStatus, StoredRepoStatus, WritableShapeMode,
        },
        snapshot::{Snapshot, SnapshotProperties},
        writer_features::{
            FeatureName, IcechunkVersion, MinVersion, WRITER_FEATURES_SINCE,
            WriterFeature,
        },
    },
    ops::{
        GCError,
        gc::{ExpiredRefAction, GCConfig, expire, garbage_collect},
    },
    repository::RepositoryErrorKind,
    session::{Session, SessionErrorKind},
    storage::{
        PutCondition, StorageContext, StorageError, StorageErrorKind, StorageResult,
        StorageWriterFeatures, VersionedUpdateResult,
        intercepting::{Intercept, InterceptingStorage},
        new_in_memory_storage,
    },
};

type TestResult<A = ()> = Result<A, Box<dyn std::error::Error>>;

const LAYOUT: WriterFeature = WriterFeature::FilesystemLayoutV1;

/// Intercepts the storage [`TestStorage::wrapped`] makes: it implements and
/// requires `filesystem-layout-v1` when `requires_layout`, and its repo-info
/// writes can be made to land and then report a fork, or to fail for missing
/// features.
#[derive(Debug)]
struct TestStorage {
    backend: Arc<dyn Storage + Send + Sync>,
    requires_layout: bool,
    fork_after_update: AtomicBool,
    refuse_for_missing_features: AtomicBool,
    repo_info_writes: AtomicUsize,
}

impl TestStorage {
    fn new(backend: Arc<dyn Storage + Send + Sync>, requires_layout: bool) -> Arc<Self> {
        Arc::new(Self {
            backend,
            requires_layout,
            fork_after_update: AtomicBool::new(false),
            refuse_for_missing_features: AtomicBool::new(false),
            repo_info_writes: AtomicUsize::new(0),
        })
    }

    /// A storage over `backend` intercepted by `self`.
    fn wrapped(self: &Arc<Self>) -> Arc<dyn Storage + Send + Sync> {
        InterceptingStorage::new(Arc::clone(&self.backend), Arc::clone(self) as _)
    }
}

#[async_trait::async_trait]
impl Intercept for TestStorage {
    async fn writer_features(
        &self,
        _backend: &(dyn Storage + Send + Sync),
    ) -> StorageResult<StorageWriterFeatures> {
        Ok(if self.requires_layout {
            StorageWriterFeatures::from([LAYOUT])
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
        if path == REPO_INFO_FILE_PATH {
            self.repo_info_writes.fetch_add(1, Ordering::SeqCst);
            if self.refuse_for_missing_features.load(Ordering::SeqCst) {
                return Err(StorageError::capture(
                    StorageErrorKind::MissingWriterFeatures {
                        missing: BTreeSet::from([FeatureName::new("test-feature-v1")]),
                        min_version: MinVersion::Newer,
                    },
                ));
            }
        }
        let res = backend
            .put_object_conditional(ctx, path, bytes, content_type, metadata, condition)
            .await?;
        match res {
            VersionedUpdateResult::Updated { new_version }
                if path == REPO_INFO_FILE_PATH
                    && self.fork_after_update.load(Ordering::SeqCst) =>
            {
                Err(StorageError::capture(
                    StorageErrorKind::RepositoryForkedAfterUpdate {
                        new_version: Box::new(new_version),
                        detail: "test fork".to_string(),
                    },
                ))
            }
            res => Ok(res),
        }
    }
}

async fn in_memory_test_storage(requires_layout: bool) -> TestResult<Arc<TestStorage>> {
    Ok(TestStorage::new(new_in_memory_storage().await?, requires_layout))
}

/// Rewrites the repo info without going through the checks, the way another
/// writer (or another storage over the same objects) would.
async fn rewrite_repo_info(
    repo: &Repository,
    change: impl FnOnce(&RepoInfo) -> IcechunkResult<RepoInfo>,
) -> TestResult {
    let am = repo.asset_manager();
    let (info, version) = am.fetch_repo_info().await?;
    write_repo_info(
        Arc::new(change(&info)?),
        am.spec_version(),
        PutCondition::IfVersion(&version),
        1,
        None,
        am.storage().as_ref(),
        &am.storage_context(),
        None,
    )
    .await?;
    Ok(())
}

fn change_features(
    add: &[&str],
    remove: &[&str],
) -> impl FnOnce(&RepoInfo) -> IcechunkResult<RepoInfo> {
    let names = |names: &[&str]| names.iter().copied().map(FeatureName::new).collect();
    let (add, remove): (Vec<_>, Vec<_>) = (names(add), names(remove));
    move |info| {
        info.with_writer_features_for_tests(
            SpecVersionBin::current(),
            &add,
            &remove,
            "test-backup",
            Utc::now(),
            100,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Commit,
    Amend,
    CreateBranch,
    ResetBranch,
    DeleteBranch,
    CreateTag,
    DeleteTag,
    SetMetadata,
    SaveConfig,
    SetFeatureFlag,
    GarbageCollect,
    Expire,
    WritableSession,
    SetStatus,
}

/// In an order where each succeeds when every check passes.
const OPS: [Op; 14] = [
    Op::Commit,
    Op::Amend,
    Op::CreateBranch,
    Op::ResetBranch,
    Op::DeleteBranch,
    Op::CreateTag,
    Op::DeleteTag,
    Op::SetMetadata,
    Op::SaveConfig,
    Op::SetFeatureFlag,
    Op::GarbageCollect,
    Op::Expire,
    Op::WritableSession,
    Op::SetStatus,
];

/// A repository with a commit on `main`, a `dev` branch, a `t1` tag, and
/// writable sessions with changes on `main` and `dev`, opened while every
/// check passed.
struct Fixture {
    repo: Repository,
    commit_session: Session,
    amend_session: Session,
    first: SnapshotId,
}

async fn fixture(storage: Arc<dyn Storage + Send + Sync>) -> TestResult<Fixture> {
    let repo = Repository::create(storage).execute().await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::root(), Bytes::new()).await?;
    let first = session.commit("first").execute().await?;
    repo.create_branch("dev", &first).await?;
    repo.create_tag("t1", &first).await?;
    let mut commit_session = repo.writable_session("main").await?;
    commit_session.add_group(Path::from_trusted("/a"), Bytes::new()).await?;
    let mut amend_session = repo.writable_session("dev").await?;
    amend_session.add_group(Path::from_trusted("/b"), Bytes::new()).await?;
    Ok(Fixture { repo, commit_session, amend_session, first })
}

fn gc_config() -> GCConfig {
    let now = Utc::now();
    GCConfig::clean_all(
        now,
        now,
        None,
        NonZeroU16::MIN.saturating_add(9),
        NonZeroUsize::MIN.saturating_add(1 << 30),
        NonZeroUsize::MIN.saturating_add(1 << 30),
        NonZeroU16::MIN.saturating_add(9),
        NonZeroU16::MIN.saturating_add(9),
        NonZeroU16::MIN.saturating_add(9),
        None,
        false,
    )
}

fn gc_kind(err: GCError) -> RepositoryErrorKind {
    match err {
        GCError::Repository(err) => err.kind,
        err => RepositoryErrorKind::Other(err.to_string()),
    }
}

/// Runs every op of [`OPS`] and returns how each ended: `None` when it
/// succeeded, the repository error kind otherwise.
async fn run_every_op(fixture: &mut Fixture) -> Vec<(Op, Option<RepositoryErrorKind>)> {
    let Fixture { repo, commit_session, amend_session, first } = fixture;
    let mut outcomes = Vec::new();
    for op in OPS {
        let res: Result<(), RepositoryErrorKind> = match op {
            Op::Commit => {
                commit_session.commit("second").execute().await.map(|_| ()).map_err(
                    |err| match err.kind {
                        SessionErrorKind::RepositoryError(kind) => kind,
                        kind => RepositoryErrorKind::Other(kind.to_string()),
                    },
                )
            }
            Op::Amend => amend_session
                .commit("amended")
                .amend()
                .execute()
                .await
                .map(|_| ())
                .map_err(|err| match err.kind {
                    SessionErrorKind::RepositoryError(kind) => kind,
                    kind => RepositoryErrorKind::Other(kind.to_string()),
                }),
            Op::CreateBranch => {
                repo.create_branch("other", first).await.map_err(|e| e.kind)
            }
            Op::DeleteBranch => repo.delete_branch("dev").await.map_err(|e| e.kind),
            Op::ResetBranch => repo
                .reset_branch("dev", &Snapshot::INITIAL_SNAPSHOT_ID, None)
                .await
                .map_err(|e| e.kind),
            Op::CreateTag => repo.create_tag("t2", first).await.map_err(|e| e.kind),
            Op::DeleteTag => repo.delete_tag("t1").await.map_err(|e| e.kind),
            Op::SetMetadata => repo
                .set_metadata(&SnapshotProperties::from_iter([(
                    "k".to_string(),
                    serde_json::Value::from(1),
                )]))
                .await
                .map_err(|e| e.kind),
            Op::SaveConfig => repo.save_config().await.map(|_| ()).map_err(|e| e.kind),
            Op::SetFeatureFlag => {
                repo.set_feature_flag("move_node", Some(false)).await.map_err(|e| e.kind)
            }
            Op::SetStatus => repo
                .set_status(&RepoStatus {
                    availability: RepoAvailability::ReadOnly,
                    set_at: Utc::now(),
                    limited_availability_reason: None,
                })
                .await
                .map_err(|e| e.kind),
            Op::GarbageCollect => {
                garbage_collect(Arc::clone(repo.asset_manager()), &gc_config(), None, 100)
                    .await
                    .map(|_| ())
                    .map_err(gc_kind)
            }
            Op::Expire => expire(
                Arc::clone(repo.asset_manager()),
                Utc::now(),
                ExpiredRefAction::Ignore,
                ExpiredRefAction::Ignore,
                None,
                100,
            )
            .await
            .map(|_| ())
            .map_err(gc_kind),
            Op::WritableSession => {
                repo.writable_session("main").await.map(|_| ()).map_err(|e| e.kind)
            }
        };
        outcomes.push((op, res.err()));
    }
    outcomes
}

/// Reads of every kind still work, and nothing was changed.
async fn assert_reads_work(repo: &Repository, first: &SnapshotId) -> TestResult {
    assert_eq!(
        repo.list_branches().await?,
        BTreeSet::from(["dev".to_string(), "main".to_string()])
    );
    assert_eq!(repo.list_tags().await?, BTreeSet::from(["t1".to_string()]));
    assert_eq!(&repo.lookup_branch("main").await?, first);
    let session = repo
        .readonly_session(&crate::repository::VersionInfo::BranchTipRef("main".into()))
        .await?;
    session.get_group(&Path::root()).await?;
    repo.get_status().await?;
    repo.get_metadata().await?;
    Ok(())
}

/// Every modification refuses with the missing feature and its version,
/// whether the feature is unknown or known but not implemented by the
/// storage (a filesystem-layout repository read through other storage); reads
/// still work.
#[tokio_test]
async fn modifications_refuse_features_the_storage_does_not_implement() -> TestResult {
    for (feature, min_version, version_text) in [
        ("test-feature-v1", MinVersion::Newer, "a newer Icechunk"),
        (LAYOUT.name(), MinVersion::Icechunk(WRITER_FEATURES_SINCE), "Icechunk 2.4.0"),
    ] {
        assert_eq!(
            WRITER_FEATURES_SINCE,
            IcechunkVersion { major: 2, minor: 4, patch: 0 }
        );
        let storage = in_memory_test_storage(false).await?;
        let mut fixture = fixture(storage.wrapped()).await?;
        rewrite_repo_info(&fixture.repo, change_features(&[feature], &[])).await?;

        for (op, err) in run_every_op(&mut fixture).await {
            let Some(RepositoryErrorKind::StorageError(
                ref kind @ StorageErrorKind::MissingWriterFeatures {
                    ref missing,
                    min_version: found,
                },
            )) = err
            else {
                panic!("{feature}: {op:?} didn't refuse for missing features: {err:?}");
            };
            assert_eq!(missing, &BTreeSet::from([FeatureName::new(feature)]), "{op:?}");
            assert_eq!(found, min_version, "{op:?}");
            let msg = kind.to_string();
            assert!(msg.contains(feature) && msg.contains(version_text), "{op:?}: {msg}");
        }
        assert_reads_work(&fixture.repo, &fixture.first).await?;
    }
    Ok(())
}

/// A storage that requires a feature the repo info doesn't list (through
/// `FilesystemStorage`: an interrupted conversion to the standard layout)
/// refuses every modification; reads still work.
#[tokio_test]
async fn modifications_refuse_an_unlisted_required_feature() -> TestResult {
    let storage = in_memory_test_storage(true).await?;
    let mut fixture = fixture(storage.wrapped()).await?;
    rewrite_repo_info(&fixture.repo, change_features(&[], &[LAYOUT.name()])).await?;

    for (op, err) in run_every_op(&mut fixture).await {
        let Some(
            ref kind @ RepositoryErrorKind::StorageError(
                StorageErrorKind::LayoutConversionInProgress(_),
            ),
        ) = err
        else {
            panic!("{op:?} didn't refuse as a conversion in progress: {err:?}");
        };
        let msg = kind.to_string();
        assert!(
            msg.contains("convert_to_standard_layout()") && msg.contains(LAYOUT.name()),
            "{op:?}: {msg}"
        );
    }
    assert_reads_work(&fixture.repo, &fixture.first).await?;
    Ok(())
}

/// Without any writer-feature problem every op of the table succeeds, so the
/// refusals above come from the checks.
#[tokio_test]
async fn every_op_succeeds_when_the_checks_pass() -> TestResult {
    for requires_layout in [false, true] {
        let storage = in_memory_test_storage(requires_layout).await?;
        let outcomes = run_every_op(&mut fixture(storage.wrapped()).await?).await;
        let failed: Vec<_> = outcomes.iter().filter(|(_, err)| err.is_some()).collect();
        assert!(failed.is_empty(), "requires_layout={requires_layout}: {failed:?}");
    }
    Ok(())
}

/// A malformed repo info is readable, not `Online`, and a format error to
/// modify; a status-only change writes a well-formed status.
#[tokio_test]
async fn malformed_shape_refuses_changes_except_the_status() -> TestResult {
    let storage = in_memory_test_storage(false).await?;
    let Fixture { repo, first, .. } = fixture(storage.wrapped()).await?;
    let set_at = Utc::now();
    rewrite_repo_info(&repo, |info| {
        info.with_raw_header_for_tests(
            vec![],
            StoredRepoStatus::for_tests(
                generated::RepoAvailability::ReadOnly,
                set_at,
                None,
                Some(StoredRepoStatus::for_tests(
                    generated::RepoAvailability::Online,
                    set_at,
                    None,
                    None,
                )),
            ),
        )
    })
    .await?;

    let err = repo.create_branch("other", &first).await.err().map(|e| e.kind);
    assert!(
        matches!(
            err,
            Some(RepositoryErrorKind::FormatError(
                IcechunkFormatErrorKind::MalformedRepoInfo(_)
            ))
        ),
        "{err:?}"
    );
    let status = repo.get_status().await?;
    assert_eq!(status.availability, RepoAvailability::ReadOnly);
    assert_reads_work(&repo, &first).await?;

    repo.set_status(&RepoStatus {
        availability: RepoAvailability::Online,
        set_at: Utc::now(),
        limited_availability_reason: None,
    })
    .await?;
    let (info, _) = repo.asset_manager().fetch_repo_info().await?;
    info.check_writable_shape(WritableShapeMode::AnyChange)?;
    assert_eq!(repo.get_status().await?.availability, RepoAvailability::Online);
    repo.create_branch("other", &first).await?;
    Ok(())
}

/// A change that lands and then finds a fork is durable: it isn't retried,
/// the cache holds it, and a commit's error carries its snapshot id.
#[tokio_test]
async fn durable_update_that_found_a_fork_is_not_retried() -> TestResult {
    let storage = in_memory_test_storage(false).await?;
    let Fixture { repo, mut commit_session, first, .. } =
        fixture(storage.wrapped()).await?;
    storage.fork_after_update.store(true, Ordering::SeqCst);
    let writes_before = storage.repo_info_writes.load(Ordering::SeqCst);

    let err = commit_session.commit("second").execute().await.err().ok_or("no error")?;
    let msg = err.kind.to_string();
    let SessionErrorKind::CommitDurableButRepositoryForked {
        snapshot, new_version, ..
    } = err.kind
    else {
        panic!("{msg}");
    };
    assert_eq!(storage.repo_info_writes.load(Ordering::SeqCst), writes_before + 1);
    for part in [
        snapshot.to_string().as_str(),
        "durable",
        "must not be retried",
        "forked",
        "recovery",
        "test fork",
    ] {
        assert!(msg.contains(part), "{part}: {msg}");
    }
    let (cached, version) =
        repo.asset_manager().cached_repo_info().ok_or("nothing cached")?;
    assert_eq!(version, *new_version);
    assert_eq!(cached.resolve_branch("main")?, snapshot);
    // The session is done: committing again can't duplicate the commit.
    let again = commit_session.commit("again").execute().await.err().map(|e| e.kind);
    assert!(matches!(again, Some(SessionErrorKind::CommitNotAllowed)), "{again:?}");

    let err = repo.create_branch("other", &first).await.err().map(|e| e.kind);
    let Some(RepositoryErrorKind::StorageError(
        StorageErrorKind::RepositoryForkedAfterUpdate { new_version, .. },
    )) = err
    else {
        panic!("{err:?}");
    };
    assert_eq!(storage.repo_info_writes.load(Ordering::SeqCst), writes_before + 2);
    let (cached, version) =
        repo.asset_manager().cached_repo_info().ok_or("nothing cached")?;
    assert_eq!(version, *new_version);
    assert_eq!(cached.resolve_branch("other")?, first);

    storage.fork_after_update.store(false, Ordering::SeqCst);
    let fresh = Repository::open(storage.wrapped()).execute().await?;
    assert_eq!(fresh.lookup_branch("main").await?, snapshot);
    assert_eq!(fresh.lookup_branch("other").await?, first);
    Ok(())
}

/// Creation lists the storage's required features, with the gate over an
/// `Online` status; through other storage the repository is read-only.
#[tokio_test]
async fn creation_lists_the_required_features() -> TestResult {
    let storage = in_memory_test_storage(true).await?;
    let repo = Repository::create(storage.wrapped()).execute().await?;
    let (info, _) = repo.asset_manager().fetch_repo_info().await?;
    assert_eq!(info.raw_writer_features()?, [FeatureName::from(LAYOUT)]);
    info.check_writable_shape(WritableShapeMode::AnyChange)?;
    assert_eq!(info.status()?.availability, RepoAvailability::Online);
    assert_eq!(repo.get_status().await?.availability, RepoAvailability::Online);
    let gate = info.raw_status()?;
    assert_eq!(gate.repo_status().availability, RepoAvailability::Online);
    assert!(serde_json::to_value(&gate)?["underlying"].is_object());
    assert_eq!(serde_json::to_value(&gate)?["availability"], "ReadOnly");

    let other = Repository::open(Arc::clone(&storage.backend)).execute().await?;
    let first = other.lookup_branch("main").await?;
    let err = other.create_branch("dev", &first).await.err().map(|e| e.kind);
    assert!(
        matches!(
            &err,
            Some(RepositoryErrorKind::StorageError(
                StorageErrorKind::MissingWriterFeatures {
                    min_version: MinVersion::Icechunk(WRITER_FEATURES_SINCE),
                    ..
                }
            ))
        ),
        "{err:?}"
    );
    assert_eq!(other.list_branches().await?, BTreeSet::from(["main".to_string()]));
    Ok(())
}

/// The storage's own missing-features error surfaces unchanged.
#[tokio_test]
async fn storage_missing_features_surfaces_unchanged() -> TestResult {
    let storage = in_memory_test_storage(false).await?;
    let Fixture { repo, first, .. } = fixture(storage.wrapped()).await?;
    storage.refuse_for_missing_features.store(true, Ordering::SeqCst);
    let err = repo.create_branch("other", &first).await.err().map(|e| e.kind);
    let Some(RepositoryErrorKind::StorageError(
        StorageErrorKind::MissingWriterFeatures { missing, min_version },
    )) = err
    else {
        panic!("{err:?}");
    };
    assert_eq!(missing, BTreeSet::from([FeatureName::new("test-feature-v1")]));
    assert_eq!(min_version, MinVersion::Newer);
    Ok(())
}

#[tokio_test]
async fn read_only_status_can_be_set_again() -> TestResult {
    let repo = Repository::create(new_in_memory_storage().await?).execute().await?;
    for reason in ["first", "second"] {
        repo.set_status(&RepoStatus {
            availability: RepoAvailability::ReadOnly,
            set_at: Utc::now(),
            limited_availability_reason: Some(reason.to_string()),
        })
        .await?;
        let status = repo.get_status().await?;
        assert_eq!(status.availability, RepoAvailability::ReadOnly);
        assert_eq!(status.limited_availability_reason.as_deref(), Some(reason));
    }
    Ok(())
}

/// An IC1 repo info can't list writer features, so a storage that requires
/// any refuses to open, and so to migrate, an IC1 repository.
#[tokio_test]
async fn a_storage_that_requires_features_refuses_ic1_repositories() -> TestResult {
    let backend = new_in_memory_storage().await?;
    Repository::create(Arc::clone(&backend))
        .spec_version(SpecVersionBin::V1)
        .execute()
        .await?;
    let err = Repository::open(TestStorage::new(backend, true).wrapped())
        .execute()
        .await
        .err()
        .map(|e| e.kind);
    assert!(matches!(err, Some(RepositoryErrorKind::StorageRefusesIc1)), "{err:?}");
    Ok(())
}
