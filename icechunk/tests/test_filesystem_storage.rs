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
    Repository, RepositoryConfig, Storage as _,
    conflicts::basic_solver::BasicConflictSolver,
    format::{
        ChunkIndices, Path, SnapshotId,
        flatbuffers::generated,
        format_constants::SpecVersionBin,
        layout_v1::{self, FeatureChange},
        manifest::ChunkPayload,
        repo_info::{RepoAvailability, RepoInfo, RepoStatus, StoredRepoStatus},
        repo_info_file,
        snapshot::ArrayShape,
        writer_features::{FeatureName, MinVersion, gate_reason},
    },
    new_in_memory_storage, new_local_filesystem_storage,
    ops::{
        GCError,
        gc::{GCConfig, GCSummary, garbage_collect},
    },
    repository::{RepositoryErrorKind, VersionInfo},
    session::SessionErrorKind,
    storage::{
        ConversionOutcome, FilesystemStorage, GetModifiedResult, PutCondition,
        StorageContext, StorageError, StorageErrorKind, VersionedUpdateResult, WriteHint,
        metering::MeteringStorage, new_filesystem_storage,
    },
};
use icechunk_macros::tokio_test;
use pretty_assertions::assert_eq;
use tokio::{sync::Barrier, task::JoinSet};
use uuid::Uuid;

use crate::common::creation_race::{RepoInfoWinner, race_creators};

const WRITERS: u32 = 16;
const COMMITS_PER_WRITER: u32 = 20;

/// Each writer opens its own `Repository` on the same directory, so the only
/// thing serializing their commits is the storage's conditional update of
/// `repo`. In every round all writers start from the same tip and commit at
/// once, so all but one must rebase. Only the changes that landed leave a
/// backup in `overwritten/`: the attempts that lost skip or delete theirs.
#[tokio_test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_commits_from_independent_repositories()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path().to_path_buf();
    let created =
        Repository::create(new_filesystem_storage(&root).await?).execute().await?;
    let (first, _) = repo_version(&created).await?;
    let initial = created.lookup_branch("main").await?;

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
            ["repo.d.tmp.", "repo.staging."].iter().any(|prefix| name.starts_with(prefix))
        })
        .collect();
    assert_eq!(leftovers, Vec::<String>::new());

    let (last, _) = repo_version(&repo).await?;
    let backups = root_names(&root.join("overwritten"))?
        .into_iter()
        .filter(|name| name.starts_with("repo."));
    assert_eq!(u64::try_from(backups.count())?, last - first);
    assert_eq!(last - first, u64::from(WRITERS * COMMITS_PER_WRITER));

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
/// - Swept: a staging file in `repo.d`, a creation directory and two root
///   staging files, all modified two days ago, and a version
///   file the marker proves dead, modified just now: dead versions go
///   whatever their age.
/// - Kept: a staging file modified just now (younger than
///   `FilesystemStorage::SWEEP_MIN_AGE`), a version file above the marker
///   (an attempt in flight) and an NFS silly-rename file.
///
/// The dry run reports the five without deleting anything. The real run
/// deletes exactly them: GC's change, which precedes the sweep, tidies up the
/// dead version, and the sweep deletes the other four. The repo still opens
/// with the same history.
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
        format!("repo.staging.{}", Uuid::new_v4()),
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
    assert_eq!(summary.storage_leftovers_deleted, 4);
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

/// A repository created through `FilesystemStorage` reads through
/// `local_filesystem_storage` and through an in-memory copy of its files, via
/// the mirror `repo`. Neither implements `filesystem-layout-v1`, so neither
/// can modify it.
#[tokio_test]
async fn other_backends_read_the_mirror_and_cannot_write() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let repo =
        Repository::create(new_filesystem_storage(dir.path()).await?).execute().await?;
    commit_group(&repo, "/group").await?;
    FilesystemStorage::new(dir.path())?.publish_mirror().await?;
    let ancestry = main_ancestry(&repo).await?;

    let copy = new_in_memory_storage().await?;
    let settings = copy.default_settings().await?;
    let ctx = StorageContext::unattributed(&settings);
    for (key, bytes) in files(dir.path())? {
        copy.put_object(&ctx, &key, bytes.into(), None, Vec::new(), WriteHint::Atomic)
            .await?;
    }

    for storage in [new_local_filesystem_storage(dir.path()).await?, copy] {
        let other = Repository::open(storage).execute().await?;
        assert_eq!(main_ancestry(&other).await?, ancestry);
        let err = other.writable_session("main").await.err().ok_or("writable")?;
        assert!(
            matches!(
                err.kind,
                RepositoryErrorKind::StorageError(
                    StorageErrorKind::MissingWriterFeatures { .. }
                )
            ),
            "{err:?}"
        );
        assert!(err.to_string().contains("filesystem-layout-v1"), "{err}");
    }
    Ok(())
}

/// Files with their `/`-separated paths.
type Files = Vec<(String, Vec<u8>)>;

/// Every file under `root`, keyed by its `/`-separated path.
fn files(root: &std::path::Path) -> Result<Files, Box<dyn Error>> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let key = path.strip_prefix(root)?.to_str().ok_or("not UTF-8")?;
                files.push((key.to_string(), std::fs::read(&path)?));
            }
        }
    }
    Ok(files)
}

/// A checked-in repository in the standard layout is refused with a pointer
/// to `convert_to_filesystem_layout()`; once converted it opens with the same
/// history, and takes commits.
#[tokio_test]
async fn standard_repo_open_names_convert() -> Result<(), Box<dyn Error>> {
    let source =
        std::env::current_dir()?.join("../icechunk-python/tests/data/test-repo-v2");
    let options = fs_extra::dir::CopyOptions::new().content_only(true);
    let standard = tempfile::tempdir()?;
    fs_extra::dir::copy(&source, standard.path(), &options)?;
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
    assert!(err.to_string().contains("convert_to_filesystem_layout()"), "{err}");

    let outcome =
        FilesystemStorage::new(dir.path())?.convert_to_filesystem_layout().await?;
    assert_eq!(outcome, ConversionOutcome::Converted);
    let converted =
        Repository::open(new_filesystem_storage(dir.path()).await?).execute().await?;
    let standard = Repository::open(new_local_filesystem_storage(standard.path()).await?)
        .execute()
        .await?;
    assert_eq!(converted.list_branches().await?, standard.list_branches().await?);
    assert_eq!(converted.list_tags().await?, standard.list_tags().await?);
    let ancestry = main_ancestry(&standard).await?;
    assert_eq!(main_ancestry(&converted).await?, ancestry);

    let snapshot = commit_group(&converted, "/after_conversion").await?;
    assert_eq!(main_ancestry(&converted).await?[1..], ancestry);
    assert_eq!(main_ancestry(&converted).await?[0], snapshot);
    Ok(())
}

/// The bytes of `$ROOT/repo` rewritten by `f`.
fn rewrite_repo(
    root: &std::path::Path,
    f: impl FnOnce(&RepoInfo) -> Result<RepoInfo, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let path = root.join("repo");
    let info = f(&repo_info_file::decode(&std::fs::read(&path)?)?)?;
    std::fs::write(&path, repo_info_file::encode(&info, SpecVersionBin::current(), 3)?)?;
    Ok(())
}

fn sorted_files(root: &std::path::Path) -> Result<Files, Box<dyn Error>> {
    let mut files = files(root)?;
    files.sort();
    Ok(files)
}

/// A standard repository listing a writer feature `FilesystemStorage`
/// doesn't implement, or with a malformed shape, is refused before anything
/// is written.
#[tokio_test]
async fn conversion_refuses_unimplemented_features_and_malformed_repo_infos()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    Repository::create(new_local_filesystem_storage(dir.path()).await?).execute().await?;
    rewrite_repo(dir.path(), |info| {
        Ok(info.with_writer_features_for_tests(
            SpecVersionBin::current(),
            &[FeatureName::new("test-feature-v1")],
            &[],
            "test-backup",
            Utc::now(),
            100,
        )?)
    })?;
    let before = sorted_files(dir.path())?;
    let err = FilesystemStorage::new(dir.path())?
        .convert_to_filesystem_layout()
        .await
        .err()
        .ok_or("converted")?;
    match err.kind {
        StorageErrorKind::MissingWriterFeatures { missing, min_version } => {
            assert_eq!(missing, BTreeSet::from([FeatureName::new("test-feature-v1")]));
            assert_eq!(min_version, MinVersion::Newer);
        }
        kind => panic!("{kind:?}"),
    }
    assert_eq!(sorted_files(dir.path())?, before);

    let dir = tempfile::tempdir()?;
    Repository::create(new_local_filesystem_storage(dir.path()).await?).execute().await?;
    let set_at = Utc::now();
    // A gate without features.
    rewrite_repo(dir.path(), |info| {
        Ok(info.with_raw_header_for_tests(
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
        )?)
    })?;
    let before = sorted_files(dir.path())?;
    let err = FilesystemStorage::new(dir.path())?
        .convert_to_filesystem_layout()
        .await
        .err()
        .ok_or("converted")?;
    assert_malformed(&err, &["status.underlying is present without writer features"]);
    assert_eq!(sorted_files(dir.path())?, before);
    Ok(())
}

/// A malformed shape refuses a conversion: no format-error kind exists among
/// the storage errors, so it is an `Other` error naming every defect.
fn assert_malformed(err: &StorageError, defects: &[&str]) {
    let StorageErrorKind::Other(msg) = &err.kind else { panic!("{err:?}") };
    assert!(msg.contains("malformed repo info"), "{msg}");
    for defect in defects {
        assert!(msg.contains(defect), "{msg}");
    }
    assert!(!msg.contains("context:"), "{msg}");
}

/// Writers commit between a crash after the conversion's directory rename and
/// its rerun. That state is the converted repository with the standard repo
/// info back in `$ROOT/repo`, which a writer's publication reports as a fork:
/// its commit is durable and the standard `$ROOT/repo` stays. With one update
/// per ops-log file, the rerun finds the conversion's backup after one commit
/// and publishes; after two, it is past the three files the provenance check
/// reads and the rerun stops.
#[tokio_test]
async fn commits_before_the_rerun_of_an_interrupted_conversion()
-> Result<(), Box<dyn Error>> {
    for (commits, found) in [(1, true), (2, false)] {
        let dir = tempfile::tempdir()?;
        let config = RepositoryConfig {
            num_updates_per_repo_info_file: Some(1),
            ..Default::default()
        };
        Repository::create(new_local_filesystem_storage(dir.path()).await?)
            .config(config)
            .execute()
            .await?;
        let standard = std::fs::read(dir.path().join("repo"))?;
        let converter = FilesystemStorage::new(dir.path())?;
        assert_eq!(
            converter.convert_to_filesystem_layout().await?,
            ConversionOutcome::Converted
        );
        std::fs::write(dir.path().join("repo"), &standard)?;

        let repo =
            Repository::open(new_filesystem_storage(dir.path()).await?).execute().await?;
        for commit in 0..commits {
            let snapshot =
                commit_reporting_a_fork(&repo, &format!("/commit_{commit}")).await?;
            assert_eq!(main_ancestry(&reopen(dir.path()).await?).await?[0], snapshot);
            assert_eq!(std::fs::read(dir.path().join("repo"))?, standard);
        }
        let (n, h) = repo_version(&repo).await?;
        let current = std::fs::read(dir.path().join("repo.d").join(format!("{n}.{h}")))?;

        let rerun = converter.convert_to_filesystem_layout().await;
        if found {
            assert_eq!(rerun?, ConversionOutcome::Converted);
            assert_eq!(std::fs::read(dir.path().join("repo"))?, current);
            assert!(layout_v1::lists_feature(&current)?);
            let mirror =
                Repository::open(new_local_filesystem_storage(dir.path()).await?)
                    .execute()
                    .await?;
            assert_eq!(main_ancestry(&mirror).await?, main_ancestry(&repo).await?);
        } else {
            let err = rerun.err().ok_or("converted")?;
            assert!(
                matches!(&err.kind, StorageErrorKind::Other(msg) if msg.contains("manual recovery")),
                "{err:?}"
            );
            assert_eq!(std::fs::read(dir.path().join("repo"))?, standard);
        }
    }
    Ok(())
}

/// Two creators, each with its own `FilesystemStorage` on one directory:
/// whichever repo info lands, its initial `flushed_at` is the stored snapshot
/// file's, although the other creator wrote that file.
#[tokio_test]
async fn concurrent_creators_agree_on_the_initial_snapshot() -> Result<(), Box<dyn Error>>
{
    for winner in [RepoInfoWinner::A, RepoInfoWinner::B] {
        let dir = tempfile::tempdir()?;
        race_creators(
            new_filesystem_storage(dir.path()).await?,
            new_filesystem_storage(dir.path()).await?,
            winner,
        )
        .await?;
    }
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

async fn reopen(root: &std::path::Path) -> Result<Repository, Box<dyn Error>> {
    Ok(Repository::open(new_filesystem_storage(root).await?).execute().await?)
}

/// A commit that lands and whose publication finds a fork: the commit is
/// durable, and its error says so with its snapshot id.
async fn commit_reporting_a_fork(
    repo: &Repository,
    path: &str,
) -> Result<SnapshotId, Box<dyn Error>> {
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::from_trusted(path), Bytes::new()).await?;
    let err = session.commit("commit").execute().await.err().ok_or("committed")?;
    match err.kind {
        SessionErrorKind::CommitDurableButRepositoryForked { snapshot, .. } => {
            Ok(snapshot)
        }
        kind => Err(format!("not a durable commit that found a fork: {kind}").into()),
    }
}

/// The bytes of the current version, read from `repo.d`.
async fn current_bytes(
    repo: &Repository,
    root: &std::path::Path,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let (n, h) = repo_version(repo).await?;
    Ok(std::fs::read(root.join("repo.d").join(format!("{n}.{h}")))?)
}

fn mirror(root: &std::path::Path) -> Result<Vec<u8>, Box<dyn Error>> {
    Ok(std::fs::read(root.join("repo"))?)
}

async fn gc(repo: &Repository) -> Result<GCSummary, GCError> {
    garbage_collect(Arc::clone(repo.asset_manager()), &clean_all(false), None, 100).await
}

/// A snapshot only a deleted branch reached: something for GC to delete.
async fn make_garbage(repo: &Repository) -> Result<(), Box<dyn Error>> {
    let tip = repo.lookup_branch("main").await?;
    repo.create_branch("garbage", &tip).await?;
    let mut session = repo.writable_session("garbage").await?;
    session.add_group(Path::from_trusted("/garbage"), Bytes::new()).await?;
    session.commit("garbage").execute().await?;
    repo.delete_branch("garbage").await?;
    Ok(())
}

/// Every file of the data directories, whose deletion would be GC's.
fn data_files(root: &std::path::Path) -> Result<BTreeSet<String>, Box<dyn Error>> {
    Ok(files(root)?
        .into_iter()
        .map(|(key, _)| key)
        .filter(|key| {
            ["chunks/", "manifests/", "snapshots/", "transactions/"]
                .iter()
                .any(|dir| key.starts_with(dir))
        })
        .collect())
}

/// The SHA-256 of `bytes`: the version `FilesystemStorage` gives a new
/// object other than `repo`.
async fn sha256_hex(
    root: &std::path::Path,
    bytes: &[u8],
) -> Result<String, Box<dyn Error>> {
    let storage = FilesystemStorage::new(root)?;
    let settings = storage.default_settings().await?;
    let ctx = StorageContext::unattributed(&settings);
    let key = format!("scratch.{}", Uuid::new_v4());
    let VersionedUpdateResult::Updated { new_version } = storage
        .put_object_conditional(
            &ctx,
            &key,
            bytes.to_vec().into(),
            None,
            Vec::new(),
            PutCondition::Create,
        )
        .await?
    else {
        return Err("the scratch object exists".into());
    };
    std::fs::remove_file(root.join(&key))?;
    Ok(new_version.etag().ok_or("no etag")?.clone())
}

fn read_only() -> RepoStatus {
    RepoStatus {
        availability: RepoAvailability::ReadOnly,
        set_at: Utc::now(),
        limited_availability_reason: None,
    }
}

/// Every change publishes its version as the mirror before returning:
/// creation, a commit, GC's change and setting the status `ReadOnly`, after
/// which commits and GC refuse and the mirror stays the `ReadOnly` version.
#[tokio_test]
async fn the_mirror_is_the_current_version_after_every_change()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let repo = Repository::create(new_filesystem_storage(root).await?).execute().await?;
    assert_eq!(mirror(root)?, current_bytes(&repo, root).await?);
    commit_group(&repo, "/a").await?;
    assert_eq!(mirror(root)?, current_bytes(&repo, root).await?);
    make_garbage(&repo).await?;
    let (before_gc, _) = repo_version(&repo).await?;
    assert!(gc(&repo).await?.snapshots_deleted > 0);
    let (after_gc, _) = repo_version(&repo).await?;
    assert!(after_gc > before_gc, "GC's change");
    assert_eq!(mirror(root)?, current_bytes(&repo, root).await?);

    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::from_trusted("/b"), Bytes::new()).await?;
    repo.set_status(&read_only()).await?;
    let read_only_version = current_bytes(&repo, root).await?;
    assert_eq!(mirror(root)?, read_only_version);
    session.commit("refused").execute().await.err().ok_or("committed")?;
    gc(&repo).await.err().ok_or("collected")?;
    assert_eq!(current_bytes(&repo, root).await?, read_only_version);
    assert_eq!(mirror(root)?, read_only_version);
    Ok(())
}

/// A fork in `$ROOT/repo`: the next commit lands and reports it, durable with
/// its snapshot id, and the fork stays. GC's change reports it too and GC
/// deletes nothing; once the fork is moved aside and the mirror published, GC
/// deletes.
#[tokio_test]
async fn a_fork_makes_commits_durable_errors_and_gc_delete_nothing()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let repo = Repository::create(new_filesystem_storage(root).await?).execute().await?;
    commit_group(&repo, "/a").await?;
    make_garbage(&repo).await?;
    let fork = layout_v1::convert(
        &mirror(root)?,
        FeatureChange::Remove,
        "fork",
        Utc::now(),
        100,
    )?;
    std::fs::write(root.join("repo"), &fork)?;

    let snapshot = commit_reporting_a_fork(&repo, "/b").await?;
    assert_eq!(main_ancestry(&reopen(root).await?).await?[0], snapshot);
    assert_eq!(mirror(root)?, fork);

    let before = data_files(root)?;
    let err = gc(&repo).await.err().ok_or("collected")?;
    assert!(
        matches!(
            &err,
            GCError::Repository(err)
                if matches!(
                    err.kind,
                    RepositoryErrorKind::StorageError(
                        StorageErrorKind::RepositoryForkedAfterUpdate { .. }
                    )
                )
        ),
        "{err:?}"
    );
    assert_eq!(data_files(root)?, before);
    assert_eq!(mirror(root)?, fork);

    std::fs::rename(root.join("repo"), root.join("fork"))?;
    FilesystemStorage::new(root)?.publish_mirror().await?;
    assert!(gc(&repo).await?.snapshots_deleted > 0);
    Ok(())
}

/// A flush whose publication finds a fork: its detached snapshot is durable,
/// the error carries its id, and the session is done as after a flush.
#[tokio_test]
async fn a_fork_makes_a_flush_a_durable_error_with_its_snapshot()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let repo = Repository::create(new_filesystem_storage(root).await?).execute().await?;
    let tip = commit_group(&repo, "/a").await?;
    let fork = layout_v1::convert(
        &mirror(root)?,
        FeatureChange::Remove,
        "fork",
        Utc::now(),
        100,
    )?;
    std::fs::write(root.join("repo"), &fork)?;

    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::from_trusted("/b"), Bytes::new()).await?;
    let err =
        session.commit("flush").anonymous().execute().await.err().ok_or("flushed")?;
    let msg = err.kind.to_string();
    let SessionErrorKind::CommitDurableButRepositoryForked { snapshot, .. } = err.kind
    else {
        panic!("not a durable flush that found a fork: {msg}");
    };
    assert!(msg.starts_with(&format!("snapshot {snapshot} is durable")), "{msg}");
    assert_eq!(mirror(root)?, fork);

    let reopened = reopen(root).await?;
    reopened.asset_manager().fetch_snapshot(&snapshot).await?;
    let (info, _) = reopened.asset_manager().fetch_repo_info().await?;
    assert_eq!(info.find_snapshot(&snapshot)?.parent_id, Some(tip.clone()));
    assert_eq!(reopened.lookup_branch("main").await?, tip);

    assert!(session.read_only());
    assert_eq!(session.snapshot_id(), &snapshot);
    assert!(!session.has_uncommitted_changes());
    let write = session.add_group(Path::from_trusted("/c"), Bytes::new()).await;
    assert!(
        matches!(write.err().map(|e| e.kind), Some(SessionErrorKind::ReadOnlySession)),
        "the session is still writable"
    );
    Ok(())
}

/// A zero-byte `$ROOT/repo` might be a fork being written: a commit lands,
/// leaves it alone and succeeds; GC refuses before deleting anything.
#[tokio_test]
async fn an_unreadable_mirror_is_left_alone_and_refuses_gc() -> Result<(), Box<dyn Error>>
{
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let repo = Repository::create(new_filesystem_storage(root).await?).execute().await?;
    make_garbage(&repo).await?;
    std::fs::write(root.join("repo"), b"")?;

    let snapshot = commit_group(&repo, "/a").await?;
    assert_eq!(main_ancestry(&reopen(root).await?).await?[0], snapshot);
    assert_eq!(mirror(root)?, b"");

    let before = data_files(root)?;
    let err = gc(&repo).await.err().ok_or("collected")?;
    assert!(
        matches!(&err, GCError::StorageError(err) if matches!(err.kind, StorageErrorKind::MirrorUnreadable(_))),
        "{err:?}"
    );
    assert_eq!(data_files(root)?, before);
    assert_eq!(mirror(root)?, b"");
    Ok(())
}

/// A current version without the feature, as an interrupted conversion to the
/// standard layout leaves it: every change and GC refuse, naming the
/// conversion to rerun; reads work; nothing is deleted.
#[tokio_test]
async fn a_featureless_current_version_refuses_every_change() -> Result<(), Box<dyn Error>>
{
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let repo = Repository::create(new_filesystem_storage(root).await?).execute().await?;
    let first = commit_group(&repo, "/a").await?;
    repo.create_tag("t1", &first).await?;
    repo.create_branch("dev", &first).await?;
    make_garbage(&repo).await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::from_trusted("/b"), Bytes::new()).await?;

    let (n, h) = repo_version(&repo).await?;
    let repo_d = root.join("repo.d");
    let converted = layout_v1::convert(
        &std::fs::read(repo_d.join(format!("{n}.{h}")))?,
        FeatureChange::Remove,
        "conversion",
        Utc::now(),
        100,
    )?;
    let h1 = sha256_hex(root, &converted).await?;
    std::fs::write(repo_d.join(format!("{}.{h1}", n + 1)), &converted)?;
    let marker =
        |n: u64, h: &str| format!("latest.{:015}.{n}.{h}", 999_999_999_999_999 - n);
    std::fs::rename(repo_d.join(marker(n, &h)), repo_d.join(marker(n + 1, &h1)))?;
    let before = data_files(root)?;

    let repo = reopen(root).await?;
    let mut refusals: Vec<(&str, String)> = vec![(
        "commit",
        session.commit("refused").execute().await.err().ok_or("committed")?.to_string(),
    )];
    let snapshot = &first;
    for (op, err) in [
        ("create_branch", repo.create_branch("other", snapshot).await.err()),
        ("delete_branch", repo.delete_branch("dev").await.err()),
        ("reset_branch", repo.reset_branch("dev", snapshot, None).await.err()),
        ("create_tag", repo.create_tag("t2", snapshot).await.err()),
        ("delete_tag", repo.delete_tag("t1").await.err()),
        ("set_status", repo.set_status(&read_only()).await.err()),
        ("save_config", repo.save_config().await.err()),
    ] {
        let err = err.ok_or(op)?;
        assert!(
            matches!(
                err.kind,
                RepositoryErrorKind::StorageError(
                    StorageErrorKind::LayoutConversionInProgress(_)
                )
            ),
            "{op}: {err}"
        );
        refusals.push((op, err.to_string()));
    }
    refusals.push(("gc", gc(&repo).await.err().ok_or("collected")?.to_string()));
    for (op, msg) in refusals {
        assert!(msg.contains("convert_to_standard_layout()"), "{op}: {msg}");
    }

    assert_eq!(main_ancestry(&repo).await?[0], first);
    assert_eq!(repo.list_tags().await?, BTreeSet::from(["t1".to_string()]));
    assert_eq!(repo.get_status().await?.availability, RepoAvailability::Online);
    assert!(data_files(root)?.is_superset(&before));
    Ok(())
}

/// Makes `bytes` the current version with plain file operations: how the
/// tests write repo infos that no change through the storage would.
async fn plant_current_version(
    repo: &Repository,
    root: &std::path::Path,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    let (n, h) = repo_version(repo).await?;
    let h1 = sha256_hex(root, bytes).await?;
    let repo_d = root.join("repo.d");
    std::fs::write(repo_d.join(format!("{}.{h1}", n + 1)), bytes)?;
    let marker =
        |n: u64, h: &str| format!("latest.{:015}.{n}.{h}", 999_999_999_999_999 - n);
    std::fs::rename(repo_d.join(marker(n, &h)), repo_d.join(marker(n + 1, &h1)))?;
    std::fs::remove_file(repo_d.join(format!("{n}.{h}")))?;
    Ok(())
}

/// A repository converted to the standard layout keeps its history, tags,
/// branches and status, opens with `new_local_filesystem_storage` and takes
/// commits there, and converts back.
#[tokio_test]
async fn conversion_to_the_standard_layout_keeps_the_repository()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let repo = Repository::create(new_filesystem_storage(root).await?).execute().await?;
    let first = commit_group(&repo, "/a").await?;
    repo.create_tag("t1", &first).await?;
    repo.create_branch("dev", &first).await?;
    commit_group(&repo, "/b").await?;
    let ancestry = main_ancestry(&repo).await?;
    repo.set_status(&read_only()).await?;
    let current = current_bytes(&repo, root).await?;

    let outcome = FilesystemStorage::new(root)?.convert_to_standard_layout().await?;
    assert_eq!(outcome, ConversionOutcome::Converted);
    assert!(!root_names(root)?.iter().any(|name| name.starts_with("repo.d")));
    let standard = mirror(root)?;
    let attempt = layout_v1::attempt_id(&standard)?.ok_or("no attempt")?;
    assert_eq!(std::fs::read(root.join("overwritten").join(attempt))?, current);

    let local =
        Repository::open(new_local_filesystem_storage(root).await?).execute().await?;
    assert_eq!(main_ancestry(&local).await?, ancestry);
    assert_eq!(local.list_tags().await?, BTreeSet::from(["t1".to_string()]));
    assert_eq!(
        local.list_branches().await?,
        BTreeSet::from(["dev".to_string(), "main".to_string()])
    );
    assert_eq!(local.get_status().await?.availability, RepoAvailability::ReadOnly);
    local
        .set_status(&RepoStatus {
            availability: RepoAvailability::Online,
            set_at: Utc::now(),
            limited_availability_reason: None,
        })
        .await?;
    let snapshot = commit_group(&local, "/c").await?;

    let outcome = FilesystemStorage::new(root)?.convert_to_filesystem_layout().await?;
    assert_eq!(outcome, ConversionOutcome::Converted);
    assert_eq!(main_ancestry(&reopen(root).await?).await?[0], snapshot);
    Ok(())
}

/// A current version whose own status is `Offline`, which reads as
/// `ReadOnly` with a synthetic reason, is written as such.
#[tokio_test]
async fn conversion_to_the_standard_layout_persists_offline_as_read_only()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let repo = Repository::create(new_filesystem_storage(root).await?).execute().await?;
    let set_at = Utc::now();
    let features = vec![FeatureName::new(layout_v1::FEATURE)];
    let gate = StoredRepoStatus::for_tests(
        generated::RepoAvailability::ReadOnly,
        set_at,
        Some(gate_reason(&features, Some("down"))),
        Some(StoredRepoStatus::for_tests(
            generated::RepoAvailability::Offline,
            set_at,
            Some("down".to_string()),
            None,
        )),
    );
    let offline = repo_info_file::decode(&current_bytes(&repo, root).await?)?
        .with_raw_header_for_tests(features, gate)?;
    let offline = repo_info_file::encode(&offline, SpecVersionBin::current(), 3)?;
    plant_current_version(&repo, root, &offline).await?;
    let read_as = repo_info_file::decode(&offline)?.status()?;
    assert_eq!(read_as.availability, RepoAvailability::ReadOnly);

    let outcome = FilesystemStorage::new(root)?.convert_to_standard_layout().await?;
    assert_eq!(outcome, ConversionOutcome::Converted);
    let standard = repo_info_file::decode(&mirror(root)?)?;
    assert_eq!(standard.status()?, read_as);
    assert_eq!(
        standard.raw_status()?,
        StoredRepoStatus::for_tests(
            generated::RepoAvailability::ReadOnly,
            read_as.set_at,
            read_as.limited_availability_reason.clone(),
            None,
        )
    );
    assert!(read_as.limited_availability_reason.is_some_and(|reason| {
        reason.starts_with("availability Offline (2) is reserved")
    }),);
    Ok(())
}

/// A current version that also lists a feature `FilesystemStorage` doesn't
/// implement is refused before anything is written.
#[tokio_test]
async fn conversion_to_the_standard_layout_refuses_unimplemented_features()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let repo = Repository::create(new_filesystem_storage(root).await?).execute().await?;
    let with_test_feature = repo_info_file::decode(&current_bytes(&repo, root).await?)?
        .with_writer_features_for_tests(
        SpecVersionBin::current(),
        &[FeatureName::new("test-feature-v1")],
        &[],
        "test-backup",
        Utc::now(),
        100,
    )?;
    let bytes = repo_info_file::encode(&with_test_feature, SpecVersionBin::current(), 3)?;
    plant_current_version(&repo, root, &bytes).await?;
    std::fs::write(root.join("repo"), &bytes)?;
    let before = sorted_files(root)?;

    let err = FilesystemStorage::new(root)?
        .convert_to_standard_layout()
        .await
        .err()
        .ok_or("converted")?;
    match err.kind {
        StorageErrorKind::MissingWriterFeatures { missing, min_version } => {
            assert_eq!(missing, BTreeSet::from([FeatureName::new("test-feature-v1")]));
            assert_eq!(min_version, MinVersion::Newer);
        }
        kind => panic!("{kind:?}"),
    }
    assert_eq!(sorted_files(root)?, before);
    Ok(())
}

/// A current version with a malformed shape is refused before anything is
/// written.
#[tokio_test]
async fn conversion_to_the_standard_layout_refuses_a_malformed_repo_info()
-> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let repo = Repository::create(new_filesystem_storage(root).await?).execute().await?;
    let feature = FeatureName::new(layout_v1::FEATURE);
    // Duplicate features and no gate.
    let malformed = repo_info_file::decode(&current_bytes(&repo, root).await?)?
        .with_raw_header_for_tests(
            vec![feature.clone(), feature],
            StoredRepoStatus::for_tests(
                generated::RepoAvailability::Online,
                Utc::now(),
                None,
                None,
            ),
        )?;
    let bytes = repo_info_file::encode(&malformed, SpecVersionBin::current(), 3)?;
    plant_current_version(&repo, root, &bytes).await?;
    std::fs::write(root.join("repo"), &bytes)?;
    let before = sorted_files(root)?;

    let err = FilesystemStorage::new(root)?
        .convert_to_standard_layout()
        .await
        .err()
        .ok_or("converted")?;
    assert_malformed(
        &err,
        &[
            "status.underlying is absent with writer features",
            "writer_features has duplicates",
        ],
    );
    assert_eq!(sorted_files(root)?, before);
    Ok(())
}

/// A conversion to the standard layout stopped after its change: every
/// change and GC through another `FilesystemStorage` refuse, naming the
/// conversion to rerun; the rerun finishes, and the repository opens in the
/// standard layout.
#[tokio_test]
async fn an_interrupted_conversion_to_the_standard_layout_refuses_changes_until_rerun()
-> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let repo = Repository::create(new_filesystem_storage(root).await?).execute().await?;
    let first = commit_group(&repo, "/a").await?;
    make_garbage(&repo).await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::from_trusted("/b"), Bytes::new()).await?;

    // The publication can't rename onto `$ROOT/repo`: the conversion stops
    // after its change.
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o555))?;
    let interrupted = FilesystemStorage::new(root)?.convert_to_standard_layout().await;
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o755))?;
    // Root renames anyway: nothing to test.
    let Err(err) = interrupted else { return Ok(()) };
    assert!(
        matches!(&err.kind, StorageErrorKind::Other(msg) if msg.contains("rerun convert_to_standard_layout()")),
        "{err:?}"
    );
    let before = data_files(root)?;

    let other = reopen(root).await?;
    let refusals = [
        session.commit("refused").execute().await.err().ok_or("committed")?.to_string(),
        commit_group(&other, "/c").await.err().ok_or("committed")?.to_string(),
        other.create_tag("t2", &first).await.err().ok_or("tagged")?.to_string(),
        gc(&other).await.err().ok_or("collected")?.to_string(),
    ];
    for msg in refusals {
        assert!(msg.contains("convert_to_standard_layout()"), "{msg}");
    }
    assert!(data_files(root)?.is_superset(&before));

    let outcome = FilesystemStorage::new(root)?.convert_to_standard_layout().await?;
    assert_eq!(outcome, ConversionOutcome::Converted);
    let local =
        Repository::open(new_local_filesystem_storage(root).await?).execute().await?;
    assert_eq!(main_ancestry(&local).await?[0], first);
    commit_group(&local, "/d").await?;
    Ok(())
}
