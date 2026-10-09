//! What `verify` does to the repository after checking it: a timed recovery
//! write through whatever the run left behind, then aging and sweeping the
//! protocol's leftovers, after which the storage root must hold only `repo`.

use std::{
    fs::{self, File},
    io,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::Context as _;
use chrono::Utc;
use icechunk::storage::{Settings, StorageContext};

use crate::{
    config::{Mode, RunConfig, WorkerId},
    layout::{RunRoot, VERIFY_TAG},
    log::{LogWriter, OpResult, start_worker_log},
    oracle::{Verdict, Violation, Warning},
    repo_mode::{self, RepoWorker},
    storage_mode::{self, Attempt, REPO_KEY, Updater},
};

/// The slot of `verify`'s recovery writes, logged as
/// `logs/verify.recovery.0.log`.
pub const RECOVERY_SLOT: &str = "recovery";

/// How far back [`age_storage_root`] sets mtimes: past
/// `FilesystemStorage::SWEEP_MIN_AGE`.
pub const AGE: Duration = Duration::from_secs(25 * 3600);

const RETRY_BACKOFF: Duration = Duration::from_millis(50);

pub fn recovery_worker() -> WorkerId {
    WorkerId {
        tag: VERIFY_TAG.to_owned(),
        slot: RECOVERY_SLOT.to_owned(),
        incarnation: 0,
    }
}

/// Long enough to wait out a lock left by a crashed writer, twice.
pub fn recovery_deadline(config: &RunConfig) -> Duration {
    2 * config.stale_after + Duration::from_secs(5)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    Succeeded { elapsed: Duration },
    Failed { elapsed: Duration, last_error: String },
}

/// One write on top of whatever the run left behind, through a fresh storage
/// without fault hooks, retried until it succeeds or `deadline` passes:
/// a conditional update of `repo` in storage mode, a commit with rebase to
/// `main` in repository mode. `Err` only if the recovery log can't be
/// written.
pub async fn recovery_write(
    root: &RunRoot,
    config: &RunConfig,
    deadline: Duration,
) -> anyhow::Result<RecoveryOutcome> {
    let who = recovery_worker();
    let log = Arc::new(start_worker_log(root, &who)?);
    let started = Instant::now();
    let until = started + deadline;
    let last_error = match config.mode {
        Mode::Storage => recover_storage(root, config, &who, &log, until).await?,
        Mode::Repository => recover_repo(root, config, &who, log, until).await?,
    };
    let elapsed = started.elapsed();
    Ok(match last_error {
        None => RecoveryOutcome::Succeeded { elapsed },
        Some(last_error) => RecoveryOutcome::Failed { elapsed, last_error },
    })
}

/// The last error, or `None` once a write succeeded.
async fn recover_storage(
    root: &RunRoot,
    config: &RunConfig,
    who: &WorkerId,
    log: &LogWriter,
    until: Instant,
) -> anyhow::Result<Option<String>> {
    let storage = storage_mode::storage(root, config, None).await?;
    let mut updater = Updater::new(storage, who, config).await?;
    let mut last_error = "no attempt finished in time".to_owned();
    while Instant::now() < until {
        let attempt = tokio::time::timeout_at(until.into(), updater.attempt(log)).await;
        last_error = match attempt {
            // An attempt cut short says nothing new.
            Err(_) => break,
            Ok(attempt) => match attempt? {
                Attempt::Logged(OpResult::Ack { .. }) => return Ok(None),
                Attempt::Logged(result) => describe(&result),
                Attempt::Raced => "repo changed while it was backed up".to_owned(),
                Attempt::Failed(err) => err,
            },
        };
        tokio::time::sleep(RETRY_BACKOFF).await;
    }
    Ok(Some(last_error))
}

/// The last error, or `None` once a commit succeeded.
async fn recover_repo(
    root: &RunRoot,
    config: &RunConfig,
    who: &WorkerId,
    log: Arc<LogWriter>,
    until: Instant,
) -> anyhow::Result<Option<String>> {
    let mut last_error = "no attempt finished in time".to_owned();
    let mut worker = None;
    while Instant::now() < until {
        let worker = match &mut worker {
            Some(worker) => worker,
            None => match tokio::time::timeout_at(
                until.into(),
                repo_mode::open_repo(root, config, None),
            )
            .await
            {
                Err(_) => {
                    last_error =
                        "opening the repository didn't finish in time".to_owned();
                    break;
                }
                Ok(Ok(repo)) => worker.insert(RepoWorker::new(
                    repo,
                    Arc::clone(&log),
                    who,
                    config.chunk_bytes,
                )?),
                Ok(Err(err)) => {
                    last_error = format!("{err:#}");
                    tokio::time::sleep(RETRY_BACKOFF).await;
                    continue;
                }
            },
        };
        let commit = tokio::time::timeout_at(until.into(), worker.main_commit()).await;
        last_error = match commit {
            Err(_) => break,
            Ok(commit) => match commit? {
                Some(OpResult::Ack { .. }) => return Ok(None),
                Some(result) => describe(&result),
                None => "preparing the commit failed (see the warning above)".to_owned(),
            },
        };
        tokio::time::sleep(RETRY_BACKOFF).await;
    }
    Ok(Some(last_error))
}

fn describe(result: &OpResult) -> String {
    match result {
        OpResult::Ack { .. } => "acknowledged".to_owned(),
        OpResult::Conflict => "conflict".to_owned(),
        OpResult::Error { message } => message.clone(),
    }
}

/// Sets the mtime of every regular file in the storage root itself, other
/// than `repo`, [`AGE`] into the past, so the sweep treats them as old.
pub fn age_storage_root(root: &RunRoot) -> anyhow::Result<()> {
    let aged = SystemTime::now() - AGE;
    for path in root_files(&root.storage())? {
        if path.file_name().is_some_and(|name| name == REPO_KEY) {
            continue;
        }
        match File::open(&path).and_then(|file| file.set_modified(aged)) {
            Err(err) if err.kind() != io::ErrorKind::NotFound => {
                return Err(err).with_context(|| format!("aging {}", path.display()));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Deletes the protocol's leftovers in the storage root: in storage mode with
/// the storage's sweep, in repository mode with a GC that also deletes
/// everything unreachable.
pub async fn sweep(root: &RunRoot, config: &RunConfig) -> anyhow::Result<()> {
    let now = Utc::now();
    match config.mode {
        Mode::Storage => {
            let storage = storage_mode::storage(root, config, None).await?;
            let settings = Settings::default();
            let ctx = StorageContext::unattributed(&settings);
            storage.sweep_leftovers(&ctx, now, false).await.context("sweeping")?;
        }
        Mode::Repository => {
            let repo = repo_mode::open_repo(root, config, None).await?;
            repo_mode::collect_garbage(&repo, config, now)
                .await
                .context("garbage collecting")?;
        }
    }
    Ok(())
}

/// Requires that the only regular file in the storage root is `repo`, and
/// warns about staging files in its subdirectories, which no sweep covers.
pub fn check_clean_root(root: &RunRoot, verdict: &mut Verdict) -> anyhow::Result<()> {
    let storage = root.storage();
    let mut names: Vec<String> = root_files(&storage)?
        .iter()
        .filter_map(|path| path.file_name())
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| name != REPO_KEY)
        .collect();
    if !names.is_empty() {
        names.sort();
        verdict.violations.push(Violation::Leftovers { names });
    }
    let mut count = 0;
    for entry in read_dir(&storage)? {
        let entry = entry.with_context(|| format!("listing {}", storage.display()))?;
        if entry.file_type()?.is_dir() {
            count += count_staging(&entry.path())?;
        }
    }
    if count > 0 {
        verdict.warnings.push(Warning::SubdirStaging { count });
    }
    Ok(())
}

fn read_dir(dir: &Path) -> anyhow::Result<fs::ReadDir> {
    fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))
}

fn root_files(dir: &Path) -> anyhow::Result<Vec<std::path::PathBuf>> {
    let mut files = Vec::new();
    for entry in read_dir(dir)? {
        let entry = entry.with_context(|| format!("listing {}", dir.display()))?;
        if entry.file_type()?.is_file() {
            files.push(entry.path());
        }
    }
    Ok(files)
}

/// Staging files anywhere below `dir`.
fn count_staging(dir: &Path) -> anyhow::Result<u64> {
    let mut count = 0;
    for entry in read_dir(dir)? {
        let entry = entry.with_context(|| format!("listing {}", dir.display()))?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            count += count_staging(&entry.path())?;
        } else if file_type.is_file()
            && entry.file_name().to_string_lossy().contains(".staging.")
        {
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use futures::TryStreamExt as _;
    use icechunk::repository::VersionInfo;
    use uuid::Uuid;

    use super::*;
    use crate::{
        config::Mode,
        log::{Op, OpResult, Record, read_log},
        oracle::{Verdict, Violation, Warning},
        repo_mode::{self, MAIN_BRANCH, parse_commit_message},
        storage_mode::{parse_payload, payload_etag},
        supervisor,
    };

    async fn initialized(mode: Mode) -> (tempfile::TempDir, RunRoot, RunConfig) {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().join("run"));
        let mut config = RunConfig::defaults(mode, 1);
        config.stale_after = Duration::from_millis(300);
        supervisor::init(&root, &config).await.unwrap();
        (dir, root, config)
    }

    /// A writer that crashed holding the lock on `repo`'s current content,
    /// with the protocol's file names.
    fn plant_holder(root: &RunRoot) {
        let storage = root.storage();
        let hash = payload_etag(&fs::read(storage.join(REPO_KEY)).unwrap());
        let holder = Uuid::new_v4();
        let lockinfo = storage.join(format!("repo.lockinfo.{holder}"));
        fs::write(storage.join(format!("repo.new_bytes.{holder}")), b"a").unwrap();
        fs::write(&lockinfo, holder.hyphenated().to_string()).unwrap();
        fs::hard_link(&lockinfo, storage.join(format!("repo.lock.{hash}.0"))).unwrap();
    }

    fn root_files(root: &RunRoot) -> BTreeSet<String> {
        fs::read_dir(root.storage())
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| entry.file_type().unwrap().is_file())
            .map(|entry| entry.file_name().into_string().unwrap())
            .collect()
    }

    fn assert_recovery_log_acked(root: &RunRoot) {
        let who = recovery_worker();
        let records = read_log(&root.worker_log(&who.tag, &who.slot, 0)).unwrap();
        assert!(matches!(records.first(), Some(Record::Start { .. })), "{records:?}");
        assert!(
            matches!(
                records.last(),
                Some(Record::Outcome { result: OpResult::Ack { .. }, .. })
            ),
            "{records:?}"
        );
    }

    async fn age_sweep_and_check(root: &RunRoot, config: &RunConfig) -> Verdict {
        age_storage_root(root).unwrap();
        sweep(root, config).await.unwrap();
        let mut verdict = Verdict::default();
        check_clean_root(root, &mut verdict).unwrap();
        verdict
    }

    #[tokio::test]
    async fn storage_recovery_takes_over_a_crashed_holder_then_the_root_is_clean() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let initial = payload_etag(&fs::read(root.storage().join(REPO_KEY)).unwrap());
        plant_holder(&root);

        let outcome = recovery_write(&root, &config, recovery_deadline(&config)).await;
        let RecoveryOutcome::Succeeded { elapsed } = outcome.unwrap() else {
            panic!("recovery failed");
        };
        assert!(elapsed >= config.stale_after, "{elapsed:?}");
        assert!(elapsed < recovery_deadline(&config), "{elapsed:?}");
        let payload =
            parse_payload(&fs::read(root.storage().join(REPO_KEY)).unwrap()).unwrap();
        assert_eq!(payload.host, VERIFY_TAG);
        assert_eq!(payload.slot, RECOVERY_SLOT);
        assert_eq!(payload.parent, Some(initial));
        assert_recovery_log_acked(&root);
        assert!(root_files(&root).len() > 1, "{:?}", root_files(&root));

        assert_eq!(age_sweep_and_check(&root, &config).await, Verdict::default());
        assert_eq!(root_files(&root), BTreeSet::from([REPO_KEY.to_owned()]));
    }

    #[tokio::test]
    async fn repository_recovery_commits_to_main_then_gc_cleans_the_root() {
        let (_dir, root, config) = initialized(Mode::Repository).await;
        plant_holder(&root);

        let outcome = recovery_write(&root, &config, recovery_deadline(&config)).await;
        assert!(matches!(outcome.unwrap(), RecoveryOutcome::Succeeded { elapsed }
                if elapsed >= config.stale_after),);
        assert_recovery_log_acked(&root);
        let repo = repo_mode::open_repo(&root, &config, None).await.unwrap();
        let main = VersionInfo::BranchTipRef(MAIN_BRANCH.to_owned());
        let ancestry: Vec<_> =
            repo.ancestry(&main).await.unwrap().try_collect().await.unwrap();
        assert!(parse_commit_message(&ancestry[0].message).is_some(), "{ancestry:?}");

        assert_eq!(age_sweep_and_check(&root, &config).await, Verdict::default());
        assert_eq!(root_files(&root), BTreeSet::from([REPO_KEY.to_owned()]));
    }

    #[tokio::test]
    async fn a_leftover_younger_than_the_aging_fails_the_clean_root_check() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let outcome = recovery_write(&root, &config, recovery_deadline(&config)).await;
        assert!(matches!(outcome.unwrap(), RecoveryOutcome::Succeeded { .. }));
        age_storage_root(&root).unwrap();
        let ticket = format!("repo.new_bytes.{}", Uuid::new_v4());
        fs::write(root.storage().join(&ticket), b"b").unwrap();
        sweep(&root, &config).await.unwrap();
        let mut verdict = Verdict::default();
        check_clean_root(&root, &mut verdict).unwrap();
        assert_eq!(verdict.violations, [Violation::Leftovers { names: vec![ticket] }]);
    }

    #[tokio::test]
    async fn recovery_that_never_succeeds_reports_the_last_error() {
        let (_dir, root, config) = initialized(Mode::Storage).await;
        let storage = root.storage();
        let hash = payload_etag(&fs::read(storage.join(REPO_KEY)).unwrap());
        fs::write(storage.join(format!("repo.lock.{hash}.0")), "garbage").unwrap();

        let deadline = Duration::from_millis(500);
        match recovery_write(&root, &config, deadline).await.unwrap() {
            RecoveryOutcome::Failed { elapsed, last_error } => {
                assert!(elapsed >= deadline, "{elapsed:?}");
                assert!(last_error.contains("lock"), "{last_error}");
            }
            RecoveryOutcome::Succeeded { .. } => panic!("recovery succeeded"),
        }
        let who = recovery_worker();
        let records = read_log(&root.worker_log(&who.tag, &who.slot, 0)).unwrap();
        assert!(
            records.iter().any(|record| matches!(
                record,
                Record::Intent { op: Op::Cas { .. }, .. }
            )),
            "{records:?}"
        );
    }

    #[tokio::test]
    async fn a_repository_open_that_hangs_fails_recovery_at_the_deadline() {
        let (_dir, root, config) = initialized(Mode::Repository).await;
        let repo = root.storage().join(REPO_KEY);
        fs::remove_file(&repo).unwrap();
        // Reading a FIFO blocks until a writer opens it.
        let status = std::process::Command::new("mkfifo").arg(&repo).status().unwrap();
        assert!(status.success());

        let deadline = Duration::from_millis(500);
        let outcome = recovery_write(&root, &config, deadline).await.unwrap();
        // Unblocks the stuck read, so the runtime can shut down.
        drop(fs::OpenOptions::new().write(true).open(&repo).unwrap());
        match outcome {
            RecoveryOutcome::Failed { elapsed, last_error } => {
                // Generous: without the bound the open never returns.
                assert!(elapsed < deadline + Duration::from_secs(10), "{elapsed:?}");
                assert!(last_error.contains("opening the repository"), "{last_error}");
            }
            RecoveryOutcome::Succeeded { .. } => panic!("recovery succeeded"),
        }
    }

    #[tokio::test]
    async fn a_fresh_repository_root_holds_only_repo() {
        let (_dir, root, _config) = initialized(Mode::Repository).await;
        assert_eq!(root_files(&root), BTreeSet::from([REPO_KEY.to_owned()]));
        let mut verdict = Verdict::default();
        check_clean_root(&root, &mut verdict).unwrap();
        assert_eq!(verdict, Verdict::default());
    }

    #[tokio::test]
    async fn staging_files_below_the_root_are_counted_as_a_warning() {
        let (_dir, root, _config) = initialized(Mode::Storage).await;
        let storage = root.storage();
        fs::create_dir_all(storage.join("a/b")).unwrap();
        fs::write(storage.join("overwritten.staging.x"), b"").unwrap();
        fs::create_dir_all(storage.join("overwritten")).unwrap();
        fs::write(storage.join("overwritten/k.staging.1"), b"").unwrap();
        fs::write(storage.join("a/b/k.staging.2"), b"").unwrap();
        fs::write(storage.join("a/b/kept"), b"").unwrap();
        let mut verdict = Verdict::default();
        check_clean_root(&root, &mut verdict).unwrap();
        assert_eq!(
            verdict.violations,
            [Violation::Leftovers { names: vec!["overwritten.staging.x".to_owned()] }]
        );
        assert_eq!(verdict.warnings, [Warning::SubdirStaging { count: 2 }]);
    }
}
