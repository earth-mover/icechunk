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
        ChunkIndices, Path, SnapshotId, manifest::ChunkPayload, snapshot::ArrayShape,
    },
    ops::gc::{GCConfig, garbage_collect},
    repository::VersionInfo,
    storage::{
        FilesystemStorage, GetModifiedResult, PutCondition, new_filesystem_storage,
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

    let names = std::fs::read_dir(&root)?
        .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<Result<Vec<_>, _>>()?;
    let leftovers: Vec<String> = names
        .into_iter()
        .filter(|name| {
            name.starts_with("repo.new_bytes.") || name.starts_with("repo.lockinfo.")
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

/// Plants protocol leftovers in the repo root, then runs GC twice.
/// - Swept: five leftovers modified two days ago (a ticket, a lockinfo, a
///   probe file, a lock for a hash `repo` doesn't hold, a staging file).
/// - Kept: a lock for the hash its object currently holds, and a lockinfo
///   modified just now (younger than `FilesystemStorage::SWEEP_MIN_AGE`).
///
/// The dry run reports the five without deleting anything; the real run
/// deletes exactly them, and the repo still opens with the same history.
///
/// The current-hash lock is on the object `target`, not on `repo`: a fresh
/// lock on `repo`'s current hash looks like a live writer's, so GC's own
/// update of `repo` would fail with a conflict.
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

    let am = repo.asset_manager();
    let ctx = am.storage_context();
    let GetModifiedResult::Modified { new_version, .. } =
        am.storage().get_object_conditional(&ctx, "repo", None).await?
    else {
        panic!("a read without a previous version returns the data");
    };
    let repo_hash = new_version.etag().ok_or("no etag")?.clone();
    let other = if repo_hash == "0".repeat(64) { "1" } else { "0" }.repeat(64);
    let target = am
        .storage()
        .put_object_conditional(
            &ctx,
            "target",
            Bytes::from_static(b"target"),
            None,
            vec![],
            PutCondition::Create,
        )
        .await?
        .must_write()?;
    let current = target.etag().ok_or("no etag")?;

    let swept = [
        format!("repo.new_bytes.{}", Uuid::new_v4()),
        format!("repo.lockinfo.{}", Uuid::new_v4()),
        format!("repo.probe.{}", Uuid::new_v4()),
        format!("repo.lock.{other}.0"),
        format!("repo.staging.{}", Uuid::new_v4()),
    ];
    let current_lock = format!("target.lock.{current}.0");
    let fresh = format!("repo.lockinfo.{}", Uuid::new_v4());
    let two_days_ago = SystemTime::now() - Duration::from_secs(2 * 24 * 60 * 60);
    for name in swept.iter().chain([&current_lock]) {
        std::fs::write(root.join(name), b"leftover")?;
        File::options().write(true).open(root.join(name))?.set_modified(two_days_ago)?;
    }
    std::fs::write(root.join(&fresh), b"leftover")?;
    let planted: BTreeSet<String> =
        swept.iter().chain([&current_lock, &fresh]).cloned().collect();

    let summary = garbage_collect(Arc::clone(am), &clean_all(true), None, 100).await?;
    assert_eq!(summary.storage_leftovers_deleted, 5);
    assert_eq!(root_names(&root)?.intersection(&planted).count(), planted.len());

    let summary = garbage_collect(Arc::clone(am), &clean_all(false), None, 100).await?;
    assert_eq!(summary.storage_leftovers_deleted, 5);
    let left: BTreeSet<String> =
        root_names(&root)?.intersection(&planted).cloned().collect();
    assert_eq!(left, BTreeSet::from([current_lock, fresh]));

    let reopened =
        Repository::open(new_filesystem_storage(&root).await?).execute().await?;
    assert_eq!(main_ancestry(&reopened).await?, ancestry);
    Ok(())
}

/// Simulates a writer that crashed while holding the lock on `repo`'s current
/// hash: a lockinfo modified two days ago, hard-linked as that lock, and left
/// alone until the lock is stale. Then runs GC twice.
/// - First run: the sweep keeps the lockinfo, GC steals the stale lock to
///   update `repo`, and only the lockinfo is left.
/// - Second run: the lockinfo no longer backs a lock, so it is swept.
///
/// The lockinfo and the lock share an inode, and lock staleness is judged by
/// the inode's ctime. Unlinking the lockinfo would bump that ctime, the lock
/// would look fresh, and GC's own update of `repo` would fail with a conflict.
#[tokio_test]
async fn gc_steals_the_lock_of_a_crashed_writer() -> Result<(), Box<dyn Error>> {
    let stale_after = Duration::from_millis(500);
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_path_buf();
    let storage = Arc::new(FilesystemStorage::new(&root)?.with_stale_after(stale_after));
    let repo = Repository::create(storage).execute().await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::root(), Bytes::new()).await?;
    session.commit("commit").execute().await?;

    let am = repo.asset_manager();
    let GetModifiedResult::Modified { new_version, .. } =
        am.storage().get_object_conditional(&am.storage_context(), "repo", None).await?
    else {
        panic!("a read without a previous version returns the data");
    };
    let repo_hash = new_version.etag().ok_or("no etag")?.clone();
    let id = Uuid::new_v4();
    let lockinfo = root.join(format!("repo.lockinfo.{id}"));
    std::fs::write(&lockinfo, id.to_string())?;
    File::options()
        .write(true)
        .open(&lockinfo)?
        .set_modified(SystemTime::now() - Duration::from_secs(2 * 24 * 60 * 60))?;
    std::fs::hard_link(&lockinfo, root.join(format!("repo.lock.{repo_hash}.0")))?;
    tokio::time::sleep(stale_after + Duration::from_millis(100)).await;

    let protocol_files = || -> Result<BTreeSet<String>, Box<dyn Error>> {
        Ok(root_names(&root)?
            .into_iter()
            .filter(|name| name.starts_with("repo.lock"))
            .collect())
    };
    garbage_collect(Arc::clone(am), &clean_all(false), None, 100).await?;
    // Stealing deleted the lock; the lockinfo is now swept like any other.
    assert_eq!(protocol_files()?, BTreeSet::from([format!("repo.lockinfo.{id}")]));
    let summary = garbage_collect(Arc::clone(am), &clean_all(false), None, 100).await?;
    assert_eq!(summary.storage_leftovers_deleted, 1);
    assert_eq!(protocol_files()?, BTreeSet::new());
    Ok(())
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
