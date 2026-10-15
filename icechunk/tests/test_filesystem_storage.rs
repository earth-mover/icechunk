use std::{
    collections::{BTreeSet, HashSet},
    error::Error,
    fs::File,
    num::{NonZeroU16, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use chrono::Utc;
use futures::TryStreamExt as _;
use icechunk::{
    Repository,
    conflicts::basic_solver::BasicConflictSolver,
    format::{
        ChunkIndices, IcechunkFormatErrorKind, Path, SnapshotId, manifest::ChunkPayload,
        snapshot::ArrayShape,
    },
    new_local_filesystem_storage,
    ops::gc::{GCConfig, garbage_collect},
    repository::{RepositoryErrorKind, VersionInfo},
    session::{SessionError, SessionErrorKind},
    storage::{
        FilesystemStorage, FreezeOutcome, GetModifiedResult, MigrateOutcome,
        StorageErrorKind, UnfreezeOutcome, metering::MeteringStorage,
        new_filesystem_storage,
    },
};
use icechunk_macros::tokio_test;
use pretty_assertions::assert_eq;
use tokio::{sync::Barrier, task::JoinSet};
use uuid::Uuid;

const WRITERS: u32 = 16;
const COMMITS_PER_WRITER: u32 = 20;

/// Each writer opens its own `Repository` on the same directory, so the only
/// thing serializing their commits is the storage's conditional update of
/// `repo`. In every round all writers start from the same tip and commit at
/// once, so all but one must rebase.
#[tokio_test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_commits_from_independent_repositories()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_path_buf();
    let initial = Repository::create(new_filesystem_storage(&root).await?)
        .execute()
        .await?
        .lookup_branch("main")
        .await?;

    let barrier = Arc::new(Barrier::new(WRITERS as usize));
    let mut set = JoinSet::new();
    for writer in 0..WRITERS {
        let root = root.clone();
        let barrier = Arc::clone(&barrier);
        set.spawn(async move { make_commits(root, writer, &barrier).await });
    }
    let mut acknowledged = HashSet::new();
    while let Some(res) = set.join_next().await {
        acknowledged.extend(res?.map_err(|e| e as Box<dyn Error>)?);
    }
    assert_eq!(acknowledged.len(), (WRITERS * COMMITS_PER_WRITER) as usize);

    let repo = Repository::open(new_filesystem_storage(&root).await?).execute().await?;
    let ancestry: Vec<SnapshotId> = repo
        .ancestry(&VersionInfo::BranchTipRef("main".to_string()))
        .await?
        .map_ok(|info| info.id)
        .try_collect()
        .await?;
    assert_eq!(ancestry.len(), (WRITERS * COMMITS_PER_WRITER + 1) as usize);
    assert_eq!(ancestry.last(), Some(&initial));
    let in_ancestry: HashSet<_> = ancestry.into_iter().collect();
    let missing: Vec<_> = acknowledged.difference(&in_ancestry).collect();
    assert_eq!(missing, Vec::<&SnapshotId>::new());

    let tip =
        repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;
    for writer in 0..WRITERS {
        let path: Path = format!("/writer_{writer}").try_into()?;
        for commit in 0..COMMITS_PER_WRITER {
            let chunk = tip.get_chunk_ref(&path, &ChunkIndices(vec![commit])).await?;
            assert_eq!(chunk, Some(payload(writer, commit)), "{path} chunk {commit}");
        }
    }

    assert_eq!(root_names(&root.join("repo.d"))?, settled(&repo).await?);
    let leftovers: Vec<String> = root_names(&root)?
        .into_iter()
        .filter(|name| {
            ["repo.stub.", "repo.d.tmp.", "repo.legacy."]
                .iter()
                .any(|prefix| name.starts_with(prefix))
        })
        .collect();
    assert_eq!(leftovers, Vec::<String>::new());

    Ok(())
}

fn payload(writer: u32, commit: u32) -> ChunkPayload {
    ChunkPayload::Inline(Bytes::from(format!("{writer}-{commit}")))
}

async fn make_commits(
    root: PathBuf,
    writer: u32,
    barrier: &Barrier,
) -> Result<Vec<SnapshotId>, Box<dyn Error + Send + Sync>> {
    let repo = Repository::open(new_filesystem_storage(&root).await?).execute().await?;
    let path: Path = format!("/writer_{writer}").try_into()?;
    let solver = BasicConflictSolver::default();
    let mut snapshots = Vec::new();
    for commit in 0..COMMITS_PER_WRITER {
        let mut session = repo.writable_session("main").await?;
        if commit == 0 {
            let shape =
                ArrayShape::new(vec![(COMMITS_PER_WRITER as u64, COMMITS_PER_WRITER)])
                    .ok_or("invalid shape")?;
            session.add_array(path.clone(), shape, None, Bytes::new()).await?;
        }
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![commit]),
                Some(payload(writer, commit)),
            )
            .await?;
        // A failing writer returns early and the test aborts the others, so
        // no one waits here forever.
        barrier.wait().await;
        // Every failed attempt is caused by another writer's commit landing,
        // so this many attempts always suffice.
        let attempts = (WRITERS * COMMITS_PER_WRITER) as u16;
        let snapshot = session
            .commit(format!("writer {writer} commit {commit}"))
            .rebase(&solver, attempts)
            .execute()
            .await?;
        snapshots.push(snapshot);
    }
    Ok(snapshots)
}

/// Plants protocol leftovers, then runs GC twice.
/// - Swept: a staging file in `repo.d`, a creation directory, a stub staging
///   file and a root staging file, all modified two days ago, and a version
///   file the marker proves dead, modified just now: dead versions go
///   whatever their age.
/// - Kept: a staging file modified just now (younger than
///   `FilesystemStorage::SWEEP_MIN_AGE`), a version file above the marker
///   (an attempt in flight) and an NFS silly-rename file.
///
/// The dry run reports the five without deleting anything; the real run
/// deletes exactly them, and the repo still opens with the same history.
#[tokio_test]
async fn gc_sweeps_root_leftovers() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_path_buf();
    let repo = Repository::create(new_filesystem_storage(&root).await?).execute().await?;
    let path: Path = "/array".try_into()?;
    for commit in 0..2 {
        let mut session = repo.writable_session("main").await?;
        if commit == 0 {
            let shape = ArrayShape::new(vec![(2, 2)]).ok_or("invalid shape")?;
            session.add_array(path.clone(), shape, None, Bytes::new()).await?;
        }
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![commit]),
                Some(payload(0, commit)),
            )
            .await?;
        session.commit(format!("commit {commit}")).execute().await?;
    }
    let ancestry = main_ancestry(&repo).await?;
    let (n, _) = repo_version(&repo).await?;

    let tmp_dir = format!("repo.d.tmp.{}", Uuid::new_v4());
    std::fs::create_dir_all(root.join(&tmp_dir))?;
    std::fs::write(root.join(&tmp_dir).join(format!("0.{}", "a".repeat(64))), b"tmp")?;
    let old = [
        format!("repo.d/stage.{}", Uuid::new_v4()),
        format!("repo.stub.{}", Uuid::new_v4()),
        format!("config.yaml.staging.{}", Uuid::new_v4()),
    ];
    let dead = format!("repo.d/{}.{}", n - 1, "b".repeat(64));
    let kept = [
        format!("repo.d/stage.{}", Uuid::new_v4()),
        format!("repo.d/{}.{}", n + 5, "c".repeat(64)),
        "repo.d/.nfs0001".to_string(),
    ];
    for name in old.iter().chain([&dead]).chain(&kept) {
        std::fs::write(root.join(name), b"leftover")?;
    }
    for name in old.iter().chain([&tmp_dir]) {
        File::open(root.join(name))?.set_modified(two_days_ago())?;
    }
    let planted: BTreeSet<String> =
        old.iter().chain([&tmp_dir, &dead]).chain(&kept).cloned().collect();

    let am = repo.asset_manager();
    let summary = garbage_collect(Arc::clone(am), &clean_all(true), None, 100).await?;
    assert_eq!(summary.storage_leftovers_deleted, 5);
    assert_eq!(repo_names(&root)?.intersection(&planted).count(), planted.len());

    let summary = garbage_collect(Arc::clone(am), &clean_all(false), None, 100).await?;
    assert_eq!(summary.storage_leftovers_deleted, 5);
    let left: BTreeSet<String> =
        repo_names(&root)?.intersection(&planted).cloned().collect();
    assert_eq!(left, BTreeSet::from(kept));

    let reopened =
        Repository::open(new_filesystem_storage(&root).await?).execute().await?;
    assert_eq!(main_ancestry(&reopened).await?, ancestry);
    Ok(())
}

/// A writer that crashed mid-commit leaves its staging file and its version
/// file `N+1` behind. Neither is a lock: the next commit through `Repository`
/// succeeds at its first attempt, and its tidy-up deletes the crashed
/// version, which its own `N+1` proves dead.
#[tokio_test]
async fn a_crashed_attempt_blocks_nobody() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_path_buf();
    let metered = Arc::new(MeteringStorage::new(new_filesystem_storage(&root).await?));
    let repo = Repository::create(Arc::clone(&metered) as _).execute().await?;
    commit_group(&repo, "/before").await?;
    let (n, _) = repo_version(&repo).await?;

    let stage = format!("stage.{}", Uuid::new_v4());
    let crashed = format!("{}.{}", n + 1, "c".repeat(64));
    for name in [&stage, &crashed] {
        std::fs::write(root.join("repo.d").join(name), b"a crashed attempt")?;
    }

    metered.reset();
    commit_group(&repo, "/after").await?;
    // Each attempt to update `repo` first backs it up.
    assert_eq!(metered.snapshot().per_op["copy_object"].requests, 1);
    let (after, _) = repo_version(&repo).await?;
    assert_eq!(after, n + 1);
    let mut expected = settled(&repo).await?;
    expected.insert(stage);
    assert_eq!(root_names(&root.join("repo.d"))?, expected);
    Ok(())
}

/// A commit on a frozen repository fails with `RepositoryFrozen` at its first
/// attempt: it makes exactly the requests of a commit that succeeds at once,
/// one backup and one conditional update of `repo` among them, and no retry.
#[tokio_test]
async fn frozen_repository_refuses_commits_without_retrying() -> Result<(), Box<dyn Error>>
{
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_path_buf();
    let metered = Arc::new(MeteringStorage::new(new_filesystem_storage(&root).await?));
    let repo = Repository::create(Arc::clone(&metered) as _).execute().await?;
    let fs = FilesystemStorage::new(&root)?;

    metered.reset();
    commit_group(&repo, "/committed").await?;
    let committed = metered.snapshot().per_op;
    assert_eq!(committed["copy_object"].requests, 1);

    let (n, h) = repo_version(&repo).await?;
    let FreezeOutcome::Frozen(frozen) = fs.freeze_repo().await? else {
        panic!("the repo was not frozen");
    };
    assert_eq!((frozen.generation(), frozen.etag()), (Some(&n.to_string()), Some(&h)));

    metered.reset();
    let err = commit_group(&repo, "/refused").await.err().ok_or("committed")?;
    let err = err.downcast_ref::<SessionError>().ok_or("not a session error")?;
    assert!(
        matches!(
            err.kind,
            SessionErrorKind::RepositoryError(RepositoryErrorKind::StorageError(
                StorageErrorKind::RepositoryFrozen
            ))
        ),
        "{err:?}"
    );
    let refused = metered.snapshot().per_op;
    for op in ["copy_object", "put_object"] {
        assert_eq!(refused[op].requests, committed[op].requests, "{op}");
    }

    assert!(matches!(fs.unfreeze_repo().await?, UnfreezeOutcome::Unfrozen(_)));
    commit_group(&repo, "/unfrozen").await?;
    assert_eq!(repo_version(&repo).await?.0, n + 2);
    Ok(())
}

/// A repository in this backend's format, opened with a backend that reads
/// `repo` as a `RepoInfo` file, names the format and how to open it.
#[tokio_test]
async fn stub_read_by_another_backend_names_the_filesystem_format()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    Repository::create(new_filesystem_storage(dir.path()).await?).execute().await?;

    let res =
        Repository::open(new_local_filesystem_storage(dir.path()).await?).execute().await;
    let err = res.err().ok_or("opened")?;
    assert!(
        matches!(
            err.kind,
            RepositoryErrorKind::FormatError(
                IcechunkFormatErrorKind::FilesystemFormatRepository
            )
        ),
        "{err:?}"
    );
    assert!(err.to_string().contains("filesystem_storage"), "{err}");
    Ok(())
}

/// A checked-in single-file repository is refused with a pointer to
/// `migrate_legacy_repo`; once migrated it opens with the same history and
/// commits.
#[tokio_test]
async fn legacy_repo_open_names_migrate() -> Result<(), Box<dyn Error>> {
    let source =
        std::env::current_dir()?.join("../icechunk-python/tests/data/test-repo-v2");
    let options = fs_extra::dir::CopyOptions::new().content_only(true);
    let legacy = tempfile::tempdir()?;
    fs_extra::dir::copy(&source, legacy.path(), &options)?;
    let dir = tempfile::tempdir()?;
    fs_extra::dir::copy(&source, dir.path(), &options)?;

    let res = Repository::open(new_filesystem_storage(dir.path()).await?).execute().await;
    let err = res.err().ok_or("opened")?;
    assert!(
        matches!(
            &err.kind,
            RepositoryErrorKind::StorageError(StorageErrorKind::Other(_))
        ),
        "{err:?}"
    );
    assert!(err.to_string().contains("migrate_legacy_repo"), "{err}");

    let outcome = FilesystemStorage::new(dir.path())?.migrate_legacy_repo().await?;
    assert_eq!(outcome, MigrateOutcome::Migrated);
    let migrated =
        Repository::open(new_filesystem_storage(dir.path()).await?).execute().await?;
    let legacy = Repository::open(new_local_filesystem_storage(legacy.path()).await?)
        .execute()
        .await?;
    assert_eq!(migrated.list_branches().await?, legacy.list_branches().await?);
    assert_eq!(migrated.list_tags().await?, legacy.list_tags().await?);
    assert_eq!(main_ancestry(&migrated).await?, main_ancestry(&legacy).await?);

    commit_group(&migrated, "/after_migration").await?;
    Ok(())
}

fn two_days_ago() -> SystemTime {
    SystemTime::now() - Duration::from_secs(2 * 24 * 60 * 60)
}

async fn commit_group(
    repo: &Repository,
    path: &str,
) -> Result<SnapshotId, Box<dyn Error>> {
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::from_trusted(path), Bytes::new()).await?;
    Ok(session.commit("commit").execute().await?)
}

/// `N` and `h` of `repo`'s current version.
async fn repo_version(repo: &Repository) -> Result<(u64, String), Box<dyn Error>> {
    let am = repo.asset_manager();
    let GetModifiedResult::Modified { new_version, .. } =
        am.storage().get_object_conditional(&am.storage_context(), "repo", None).await?
    else {
        panic!("a read without a previous version returns the data");
    };
    let n = new_version.generation().ok_or("no generation")?.parse()?;
    Ok((n, new_version.etag().ok_or("no etag")?.clone()))
}

/// The names `repo.d` holds when nothing is in flight: the current version
/// and its marker.
async fn settled(repo: &Repository) -> Result<BTreeSet<String>, Box<dyn Error>> {
    let (n, h) = repo_version(repo).await?;
    Ok(BTreeSet::from([
        format!("{n}.{h}"),
        format!("latest.{:015}.{n}.{h}", 999_999_999_999_999 - n),
    ]))
}

fn clean_all(dry_run: bool) -> GCConfig {
    let now = Utc::now();
    GCConfig::clean_all(
        now,
        now,
        None,
        NonZeroU16::new(50).unwrap(),
        NonZeroUsize::new(512 * 1024 * 1024).unwrap(),
        NonZeroUsize::new(4 * 1024 * 1024 * 1024).unwrap(),
        NonZeroU16::new(500).unwrap(),
        NonZeroU16::new(10).unwrap(),
        NonZeroU16::new(50).unwrap(),
        None,
        dry_run,
    )
}

async fn main_ancestry(repo: &Repository) -> Result<Vec<SnapshotId>, Box<dyn Error>> {
    Ok(repo
        .ancestry(&VersionInfo::BranchTipRef("main".to_string()))
        .await?
        .map_ok(|info| info.id)
        .try_collect()
        .await?)
}

fn root_names(root: &std::path::Path) -> Result<BTreeSet<String>, Box<dyn Error>> {
    Ok(std::fs::read_dir(root)?
        .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_, _>>()?)
}

/// The names in `root` and, as `repo.d/<name>`, in `repo.d`.
fn repo_names(root: &std::path::Path) -> Result<BTreeSet<String>, Box<dyn Error>> {
    let mut names = root_names(root)?;
    names.extend(
        root_names(&root.join("repo.d"))?
            .into_iter()
            .map(|name| format!("repo.d/{name}")),
    );
    Ok(names)
}
