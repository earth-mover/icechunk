//! `local_filesystem_storage` refuses GC and every write of `repo` while
//! `$ROOT/repo.d` exists, whatever `$ROOT/repo` holds.

use std::{error::Error, fmt, path::Path as StdPath};

use bytes::Bytes;
use chrono::{Duration, Utc};
use icechunk::{
    Repository,
    asset_manager::force_write_repo_info,
    format::{
        CHUNKS_FILE_PATH, ChunkId, Path, SnapshotId,
        repo_info::{RepoAvailability, RepoStatus},
        repo_info_file,
        writer_features::WriterFeature,
    },
    new_local_filesystem_storage,
    ops::gc::{Action, GCConfig, garbage_collect},
    storage::{GcPhase, VersionInfo as StorageVersion, WriteHint},
};
use icechunk_macros::tokio_test;
use pretty_assertions::assert_eq;

type TestResult<A> = Result<A, Box<dyn Error>>;

const GUARD_PHRASE: &str = "local_filesystem_storage refuses";
const MISSING_FEATURES_PHRASE: &str =
    "writer features that this storage doesn't implement";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepoFile {
    Featureless,
    ListingTheFeature,
    Missing,
    Undecodable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    OpenAndListBranches,
    ReadRepoFile,
    DeleteOtherObject,
    Commit,
    CreateBranch,
    DeleteBranch,
    SetStatus,
    ForceWriteRepoInfo,
    CreateWithoutCleanRootCheck,
    CopyObjectToRepo,
    GcHookBeforeDeletes { changed: bool },
    GcHookAfterDeletes { changed: bool },
    GarbageCollect { drops_snapshots: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Ok,
    Guard,
    /// Core's writer-feature check, which runs before the guard.
    MissingWriterFeatures,
    OtherError,
}

#[derive(PartialEq, Eq)]
struct Cell {
    repo_d: bool,
    repo_file: RepoFile,
    op: Op,
    outcome: Outcome,
}

impl fmt::Debug for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let repo_d = if self.repo_d { "repo.d" } else { "no repo.d" };
        write!(f, "{repo_d}, {:?}, {:?} -> {:?}", self.repo_file, self.op, self.outcome)
    }
}

const WRITES: [Op; 13] = [
    Op::Commit,
    Op::CreateBranch,
    Op::DeleteBranch,
    Op::SetStatus,
    Op::ForceWriteRepoInfo,
    Op::CreateWithoutCleanRootCheck,
    Op::CopyObjectToRepo,
    Op::GcHookBeforeDeletes { changed: false },
    Op::GcHookBeforeDeletes { changed: true },
    Op::GcHookAfterDeletes { changed: false },
    Op::GcHookAfterDeletes { changed: true },
    Op::GarbageCollect { drops_snapshots: true },
    Op::GarbageCollect { drops_snapshots: false },
];

const READS: [Op; 3] = [Op::OpenAndListBranches, Op::ReadRepoFile, Op::DeleteOtherObject];

const OTHER_OBJECT: &str = "guard-test/source";

/// A standard repository with one commit on `main` and a `dev` branch, and
/// the bytes of its repo info rewritten to list `filesystem-layout-v1`.
struct Template {
    dir: tempfile::TempDir,
    main_tip: SnapshotId,
    listing_the_feature: Vec<u8>,
}

async fn template() -> TestResult<Template> {
    let dir = tempfile::tempdir()?;
    let repo = Repository::create(new_local_filesystem_storage(dir.path()).await?)
        .execute()
        .await?;
    let mut session = repo.writable_session("main").await?;
    session.add_group(Path::from_trusted("/a"), Bytes::new()).await?;
    let main_tip = session.commit("first").execute().await?;
    repo.create_branch("dev", &main_tip).await?;
    let ctx = repo.storage_context();
    repo.storage()
        .put_object(
            &ctx,
            OTHER_OBJECT,
            Bytes::from_static(b"source"),
            None,
            Vec::new(),
            WriteHint::Atomic,
        )
        .await?;

    let (info, _) = repo.asset_manager().fetch_repo_info().await?;
    let with_feature = info.with_writer_features(
        repo.spec_version(),
        &[WriterFeature::FilesystemLayoutV1],
        &[],
        &repo_info_file::new_backup_path(),
        Utc::now(),
        repo.config().num_updates_per_repo_info_file(),
    )?;
    let listing_the_feature = repo_info_file::encode(
        &with_feature,
        repo.spec_version(),
        repo.config().compression().level(),
    )?;
    Ok(Template { dir, main_tip, listing_the_feature })
}

/// Deletes every unreachable object, dropping snapshots only when
/// `drops_snapshots`: without that, GC on a standard repository makes no change
/// and the hook before its deletes is the first write it attempts.
fn gc_config(drops_snapshots: bool) -> GCConfig {
    let cutoff = Utc::now() + Duration::hours(1);
    let metadata = if drops_snapshots {
        Action::DeleteIfCreatedBefore(cutoff)
    } else {
        Action::Keep
    };
    let limit = std::num::NonZeroU16::new(10).unwrap_or(std::num::NonZeroU16::MIN);
    let mem = std::num::NonZeroUsize::new(1 << 30).unwrap_or(std::num::NonZeroUsize::MIN);
    GCConfig::new(
        Default::default(),
        Action::DeleteIfCreatedBefore(cutoff),
        Action::Keep,
        Action::Keep,
        Action::Keep,
        metadata,
        Action::DeleteIfCreatedBefore(cutoff),
        limit,
        mem,
        mem,
        limit,
        limit,
        limit,
        None,
        false,
    )
}

fn read_repo_file(root: &StdPath) -> TestResult<Option<Vec<u8>>> {
    match std::fs::read(root.join("repo")) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Runs `op` on a copy of the template whose `repo` is `repo_file`, through a
/// `Repository` opened before `repo.d` and `repo_file` were put in place.
async fn run(
    template: &Template,
    repo_d: bool,
    repo_file: RepoFile,
    op: Op,
) -> TestResult<(Outcome, String)> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let options = fs_extra::dir::CopyOptions::new().content_only(true);
    fs_extra::dir::copy(template.dir.path(), root, &options)?;
    let repo =
        Repository::open(new_local_filesystem_storage(root).await?).execute().await?;
    let (template_info, _) = repo.asset_manager().fetch_repo_info().await?;

    if repo_d {
        std::fs::create_dir_all(root.join("repo.d"))?;
    }
    match repo_file {
        RepoFile::Featureless => {}
        RepoFile::ListingTheFeature => {
            std::fs::write(root.join("repo"), &template.listing_the_feature)?;
        }
        RepoFile::Missing => std::fs::remove_file(root.join("repo"))?,
        RepoFile::Undecodable => std::fs::write(root.join("repo"), b"not a repo info")?,
    }
    let before = read_repo_file(root)?;
    let garbage = root.join(CHUNKS_FILE_PATH).join(ChunkId::random().to_string());
    if matches!(op, Op::GarbageCollect { .. }) {
        std::fs::create_dir_all(root.join(CHUNKS_FILE_PATH))?;
        std::fs::write(&garbage, b"garbage")?;
    }

    let storage = repo.storage();
    let ctx = repo.storage_context();
    let changed = StorageVersion::for_creation();
    let changed = |c: bool| c.then_some(&changed);
    let res: TestResult<()> = async {
        match op {
            Op::OpenAndListBranches => {
                Repository::open(new_local_filesystem_storage(root).await?)
                    .execute()
                    .await?
                    .list_branches()
                    .await?;
            }
            Op::ReadRepoFile => {
                storage.get_object(&ctx, "repo", None).await?;
            }
            Op::DeleteOtherObject => {
                let (prefix, id) = OTHER_OBJECT.split_once('/').ok_or("bad path")?;
                let res =
                    storage.delete_batch(&ctx, prefix, vec![(id.to_string(), 6)]).await?;
                if res.deleted_objects != 1 || root.join(OTHER_OBJECT).exists() {
                    return Err("not deleted".into());
                }
            }
            Op::Commit => {
                let mut session = repo.writable_session("main").await?;
                session.add_group(Path::from_trusted("/b"), Bytes::new()).await?;
                session.commit("second").execute().await?;
            }
            Op::CreateBranch => {
                repo.create_branch("other", &template.main_tip).await?;
            }
            Op::DeleteBranch => repo.delete_branch("dev").await?,
            Op::SetStatus => {
                repo.set_status(&RepoStatus {
                    availability: RepoAvailability::ReadOnly,
                    set_at: Utc::now(),
                    limited_availability_reason: None,
                })
                .await?;
            }
            Op::ForceWriteRepoInfo => {
                // SAFETY: nothing else writes this repository
                #[expect(unsafe_code)]
                unsafe {
                    force_write_repo_info(
                        template_info,
                        repo.spec_version(),
                        repo.config().compression().level(),
                        storage.as_ref(),
                        &ctx,
                        None,
                    )
                    .await?;
                }
            }
            Op::CreateWithoutCleanRootCheck => {
                Repository::create(new_local_filesystem_storage(root).await?)
                    .check_clean_root(false)
                    .execute()
                    .await?;
            }
            Op::CopyObjectToRepo => {
                storage
                    .copy_object(
                        &ctx,
                        OTHER_OBJECT,
                        "repo",
                        None,
                        &StorageVersion::for_creation(),
                    )
                    .await?;
            }
            Op::GcHookBeforeDeletes { changed: c } => {
                storage
                    .gc_hook(&ctx, GcPhase::BeforeDeletes { changed: changed(c) })
                    .await?;
            }
            Op::GcHookAfterDeletes { changed: c } => {
                storage
                    .gc_hook(&ctx, GcPhase::AfterDeletes { changed: changed(c) })
                    .await?;
            }
            Op::GarbageCollect { drops_snapshots } => {
                garbage_collect(
                    std::sync::Arc::clone(repo.asset_manager()),
                    &gc_config(drops_snapshots),
                    None,
                    repo.config().num_updates_per_repo_info_file(),
                )
                .await?;
            }
        }
        Ok(())
    }
    .await;

    if matches!(op, Op::GarbageCollect { .. }) && garbage.exists() == res.is_ok() {
        let state = if res.is_ok() { "succeeded but kept" } else { "failed but deleted" };
        return Err(format!("{op:?} {state} the garbage chunk").into());
    }
    let Err(err) = res else { return Ok((Outcome::Ok, String::new())) };
    if read_repo_file(root)? != before {
        return Err(format!("{op:?} failed but changed repo").into());
    }
    let msg = err.to_string();
    let outcome = if msg.contains(GUARD_PHRASE) {
        Outcome::Guard
    } else if msg.contains(MISSING_FEATURES_PHRASE) {
        Outcome::MissingWriterFeatures
    } else {
        Outcome::OtherError
    };
    Ok((outcome, msg))
}

/// Every write of `repo` and GC are refused with the guard's message while
/// `repo.d` exists, whatever `repo` holds (unless core refuses first);
/// reads and deletes are unaffected; without `repo.d` everything works.
#[tokio_test]
async fn local_filesystem_storage_refuses_writes_of_repo_while_repo_d_exists()
-> TestResult<()> {
    let template = template().await?;
    let mut expected = Vec::new();
    for repo_file in [
        RepoFile::Featureless,
        RepoFile::ListingTheFeature,
        RepoFile::Missing,
        RepoFile::Undecodable,
    ] {
        for op in READS {
            let readable = match op {
                Op::OpenAndListBranches => {
                    matches!(
                        repo_file,
                        RepoFile::Featureless | RepoFile::ListingTheFeature
                    )
                }
                Op::ReadRepoFile => repo_file != RepoFile::Missing,
                _ => true,
            };
            let outcome = if readable { Outcome::Ok } else { Outcome::OtherError };
            expected.push(Cell { repo_d: true, repo_file, op, outcome });
        }
        for op in WRITES {
            // Core reads `repo` and checks its writer features before every
            // change, so these fail before writing.
            let core_change = matches!(
                op,
                Op::Commit
                    | Op::CreateBranch
                    | Op::DeleteBranch
                    | Op::SetStatus
                    | Op::GarbageCollect { .. }
            );
            let outcome = match repo_file {
                RepoFile::Missing | RepoFile::Undecodable if core_change => {
                    Outcome::OtherError
                }
                RepoFile::ListingTheFeature if core_change => {
                    Outcome::MissingWriterFeatures
                }
                _ => Outcome::Guard,
            };
            expected.push(Cell { repo_d: true, repo_file, op, outcome });
        }
    }
    for op in READS.into_iter().chain(WRITES) {
        let repo_file = if op == Op::CreateWithoutCleanRootCheck {
            RepoFile::Missing
        } else {
            RepoFile::Featureless
        };
        expected.push(Cell { repo_d: false, repo_file, op, outcome: Outcome::Ok });
    }

    let mut actual = Vec::new();
    let mut messages = Vec::new();
    for &Cell { repo_d, repo_file, op, .. } in &expected {
        let (outcome, msg) = run(&template, repo_d, repo_file, op).await?;
        actual.push(Cell { repo_d, repo_file, op, outcome });
        messages.push(format!("repo.d={repo_d} {repo_file:?} {op:?}: {msg}"));
    }
    assert_eq!(expected, actual, "{}", messages.join("\n"));

    let guard = messages.iter().find(|m| m.contains(GUARD_PHRASE)).ok_or("no guard")?;
    for part in ["repo.d", "filesystem layout", "filesystem_storage", "Forks"] {
        assert!(guard.contains(part), "{guard}");
    }
    Ok(())
}
