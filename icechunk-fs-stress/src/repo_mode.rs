//! Repository mode: workers commit through icechunk's [`Repository`] API on
//! top of the run's storage, normally the hooked
//! [`FilesystemStorage`](icechunk_fs::FilesystemStorage).
//! Each worker commits to its own branch, commits with rebase to `main`, and
//! tags its branch commits; every commit message names the intent it
//! carries out, so the oracle can match snapshots to log records.

use std::{
    num::{NonZeroU16, NonZeroUsize},
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use anyhow::Context as _;
use bytes::Bytes;
use chrono::{TimeDelta, Utc};
use icechunk::{
    Repository, RepositoryConfig,
    config::RepoUpdateRetryConfig,
    conflicts::basic_solver::BasicConflictSolver,
    format::{
        ChunkIndices, IcechunkFormatErrorKind, Path, SnapshotId,
        snapshot::{ArrayShape, Snapshot},
    },
    ops::gc::{GCConfig, GCResult, GCSummary, garbage_collect},
    refs::RefErrorKind,
    repository::{RepositoryError, RepositoryErrorKind},
    session::{Session, SessionError, SessionErrorKind},
    storage::RetriesSettings,
};
use icechunk_fs::fault_injection::CasHooks;
use rand::{RngExt as _, rngs::StdRng};
use uuid::Uuid;

use crate::{
    config::{RetryConfig, RunConfig, WorkerId},
    control::StopCheck,
    faults::FaultHooks,
    layout::{RunRoot, is_valid_slot, is_valid_tag},
    log::{LogWriter, Op, OpResult, Record, now_ms, start_worker_log},
    storage_mode,
};

pub const MAIN_BRANCH: &str = "main";

const BRANCH_PREFIX: &str = "w.";
const MESSAGE_PREFIX: &str = "stress ";
const TAG_PREFIX: &str = "t-";

pub fn branch_name(tag: &str, slot: &str) -> String {
    format!("{BRANCH_PREFIX}{tag}.{slot}")
}

/// The host tag and slot of a worker branch name.
pub fn parse_branch_name(name: &str) -> Option<(&str, &str)> {
    let (tag, slot) = name.strip_prefix(BRANCH_PREFIX)?.rsplit_once('.')?;
    (is_valid_tag(tag) && is_valid_slot(slot)).then_some((tag, slot))
}

pub fn array_path_on_branch(tag: &str, slot: &str) -> anyhow::Result<Path> {
    array_path(&format!("/w.{tag}.{slot}"))
}

pub fn array_path_on_main(tag: &str, slot: &str) -> anyhow::Result<Path> {
    array_path(&format!("/m.{tag}.{slot}"))
}

fn array_path(path: &str) -> anyhow::Result<Path> {
    Path::new(path).map_err(|err| anyhow::anyhow!("invalid array path {path:?}: {err}"))
}

pub fn tag_name(id: Uuid) -> String {
    format!("{TAG_PREFIX}{id}")
}

/// The hyphenated `id` repeated and truncated to `n` bytes.
pub fn chunk_bytes(id: Uuid, n: usize) -> Bytes {
    id.hyphenated().to_string().into_bytes().into_iter().cycle().take(n).collect()
}

pub fn commit_message(id: Uuid) -> String {
    format!("{MESSAGE_PREFIX}{id}")
}

/// The id in a message [`commit_message`] wrote, and only in such a message.
pub fn parse_commit_message(message: &str) -> Option<Uuid> {
    let id = message.strip_prefix(MESSAGE_PREFIX)?;
    Uuid::try_parse(id).ok().filter(|uuid| uuid.hyphenated().to_string() == id)
}

fn retries_settings(retries: &RetryConfig) -> RetriesSettings {
    RetriesSettings {
        max_tries: Some(retries.max_tries),
        initial_backoff_ms: Some(retries.initial_backoff_ms),
        max_backoff_ms: Some(retries.max_backoff_ms),
    }
}

fn repo_update_retries(config: &RunConfig) -> RepoUpdateRetryConfig {
    RepoUpdateRetryConfig { default: Some(retries_settings(&config.repo_update_retries)) }
}

fn repository_config(config: &RunConfig) -> RepositoryConfig {
    RepositoryConfig {
        repo_update_retries: Some(repo_update_retries(config)),
        ..RepositoryConfig::default()
    }
}

/// Opens the run's repository, with `repo_update_retries` from the run
/// configuration.
pub async fn open_repo(
    root: &RunRoot,
    config: &RunConfig,
    hooks: Option<Arc<dyn CasHooks>>,
) -> anyhow::Result<Repository> {
    let storage = storage_mode::storage(root, config, hooks).await?;
    Repository::open(storage)
        .config(repository_config(config))
        .execute()
        .await
        .map_err(|err| anyhow::anyhow!("opening the repository: {}", err.kind()))
}

/// Creates the repository, with only `main` at the initial snapshot.
pub async fn init(root: &RunRoot, config: &RunConfig) -> anyhow::Result<()> {
    let storage = storage_mode::storage(root, config, None).await?;
    Repository::create(storage)
        .config(repository_config(config))
        .execute()
        .await
        .map_err(|err| anyhow::anyhow!("creating the repository: {}", err.kind()))?;
    Ok(())
}

/// The outcome to log for a failed operation.
pub fn classify(err: &SessionError) -> OpResult {
    let conflict = match err.kind() {
        SessionErrorKind::Conflict { .. } | SessionErrorKind::RebaseFailed { .. } => true,
        SessionErrorKind::RepositoryError(kind) => is_conflict(kind),
        _ => false,
    };
    outcome(conflict, err.kind())
}

/// [`classify`] for the errors of [`Repository`] operations.
pub fn classify_repository(err: &RepositoryError) -> OpResult {
    outcome(is_conflict(err.kind()), err.kind())
}

fn is_conflict(kind: &RepositoryErrorKind) -> bool {
    matches!(
        kind,
        RepositoryErrorKind::RepoUpdateAttemptsLimit(_)
            | RepositoryErrorKind::Ref(RefErrorKind::TagAlreadyExists(_))
    )
}

/// The message leaves out the error's span trace.
fn outcome(conflict: bool, kind: &impl std::fmt::Display) -> OpResult {
    if conflict {
        OpResult::Conflict
    } else {
        OpResult::Error { message: kind.to_string() }
    }
}

const ERROR_BACKOFF: Duration = Duration::from_millis(100);
const REBASE_ATTEMPTS: u16 = 100;
const BRANCH_COMMIT_SHARE: f64 = 0.6;
const MAIN_COMMIT_SHARE: f64 = 0.3;

/// One worker incarnation in repository mode. Each step logs an intent, runs
/// one repository operation and logs its outcome.
#[derive(Debug)]
pub struct RepoWorker {
    repo: Repository,
    log: Arc<LogWriter>,
    branch: String,
    branch_array: Path,
    main_array: Path,
    shape: ArrayShape,
    chunk_len: usize,
    last_branch_snapshot: Option<SnapshotId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    OwnBranch,
    Main,
}

impl RepoWorker {
    pub fn new(
        repo: Repository,
        log: Arc<LogWriter>,
        who: &WorkerId,
        chunk_len: usize,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            repo,
            log,
            branch: branch_name(&who.tag, &who.slot),
            branch_array: array_path_on_branch(&who.tag, &who.slot)?,
            main_array: array_path_on_main(&who.tag, &who.slot)?,
            // One chunk; icechunk doesn't interpret its bytes.
            shape: ArrayShape::new([(1, 1)]).context("invalid array shape")?,
            chunk_len,
            last_branch_snapshot: None,
        })
    }

    pub fn branch(&self) -> &str {
        &self.branch
    }

    /// Creates the worker's branch at the initial snapshot unless it exists.
    /// Returns whether the create was acked or the branch already existed;
    /// `Err` only if the log can't be written.
    pub async fn ensure_branch(&self) -> anyhow::Result<bool> {
        match self.repo.lookup_branch(&self.branch).await {
            Ok(_) => return Ok(true),
            Err(err)
                if matches!(
                    err.kind(),
                    RepositoryErrorKind::Ref(RefErrorKind::RefNotFound(_))
                ) => {}
            Err(err) => {
                tracing::warn!("looking up {}: {}", self.branch, err.kind());
                return Ok(false);
            }
        }
        let id = Uuid::new_v4();
        let op = Op::CreateBranch { branch: self.branch.clone() };
        self.log.append(&Record::Intent { t_ms: now_ms(), id, op })?;
        let result = match self
            .repo
            .create_branch(&self.branch, &Snapshot::INITIAL_SNAPSHOT_ID)
            .await
        {
            Ok(()) => OpResult::Ack { snapshot: None },
            // The lookup found no branch, so icechunk retried after an
            // attempt that landed but reported a conflict, as for a tag.
            Err(err)
                if matches!(
                    err.kind(),
                    RepositoryErrorKind::FormatError(
                        IcechunkFormatErrorKind::BranchAlreadyExists { .. }
                    )
                ) =>
            {
                OpResult::Conflict
            }
            Err(err) => classify_repository(&err),
        };
        let created = matches!(result, OpResult::Ack { .. });
        self.log.append(&Record::Outcome { t_ms: now_ms(), id, result })?;
        Ok(created)
    }

    /// Commits a new chunk to the worker's array on its branch. `None` if the
    /// commit couldn't be prepared, so no intent was logged.
    pub async fn branch_commit(&mut self) -> anyhow::Result<Option<OpResult>> {
        self.commit(Target::OwnBranch).await
    }

    /// Commits a new chunk to the worker's array on `main`, rebasing onto the
    /// other workers' commits.
    pub async fn main_commit(&mut self) -> anyhow::Result<Option<OpResult>> {
        self.commit(Target::Main).await
    }

    /// Tags the last acknowledged branch commit; `None` if there is none yet.
    pub async fn tag(&self) -> anyhow::Result<Option<OpResult>> {
        let Some(snapshot) = &self.last_branch_snapshot else {
            return Ok(None);
        };
        let id = Uuid::new_v4();
        let name = tag_name(id);
        let op = Op::Tag { name: name.clone(), snapshot: snapshot.to_string() };
        self.log.append(&Record::Intent { t_ms: now_ms(), id, op })?;
        let result = match self.repo.create_tag(&name, snapshot).await {
            Ok(()) => OpResult::Ack { snapshot: Some(snapshot.to_string()) },
            Err(err) => classify_repository(&err),
        };
        self.log.append(&Record::Outcome {
            t_ms: now_ms(),
            id,
            result: result.clone(),
        })?;
        Ok(Some(result))
    }

    async fn commit(&mut self, target: Target) -> anyhow::Result<Option<OpResult>> {
        let id = Uuid::new_v4();
        let (branch, op) = match target {
            Target::OwnBranch => {
                (self.branch.as_str(), Op::BranchCommit { branch: self.branch.clone() })
            }
            Target::Main => (MAIN_BRANCH, Op::MainCommit),
        };
        let mut session = match self.prepare(target, id).await {
            Ok(session) => session,
            Err(err) => {
                tracing::warn!("preparing a commit to {branch}: {}", err.kind());
                tokio::time::sleep(ERROR_BACKOFF).await;
                return Ok(None);
            }
        };
        self.log.append(&Record::Intent { t_ms: now_ms(), id, op })?;
        let solver = BasicConflictSolver::default();
        let commit = session.commit(commit_message(id));
        // Nobody else writes the worker's branch, so only `main` rebases.
        let commit = match target {
            Target::OwnBranch => commit,
            Target::Main => commit.rebase(&solver, REBASE_ATTEMPTS),
        };
        let result = match commit.execute().await {
            Ok(snapshot) => {
                let result = OpResult::Ack { snapshot: Some(snapshot.to_string()) };
                if target == Target::OwnBranch {
                    self.last_branch_snapshot = Some(snapshot);
                }
                result
            }
            Err(err) => classify(&err),
        };
        self.log.append(&Record::Outcome {
            t_ms: now_ms(),
            id,
            result: result.clone(),
        })?;
        Ok(Some(result))
    }

    /// A session on the target branch with the worker's array (created if
    /// missing) holding a fresh chunk for `id`.
    async fn prepare(&self, target: Target, id: Uuid) -> Result<Session, SessionError> {
        let (branch, array) = match target {
            Target::OwnBranch => (self.branch.as_str(), &self.branch_array),
            Target::Main => (MAIN_BRANCH, &self.main_array),
        };
        let mut session =
            self.repo.writable_session(branch).await.map_err(|err| err.inject())?;
        match session.get_array(array).await {
            Ok(_) => {}
            Err(SessionError { kind: SessionErrorKind::NodeNotFound { .. }, .. }) => {
                let shape = self.shape.clone();
                session.add_array(array.clone(), shape, None, Bytes::new()).await?;
            }
            Err(err) => return Err(err),
        }
        let index = ChunkIndices(vec![0]);
        let payload =
            session.get_chunk_writer(array, &index)?(chunk_bytes(id, self.chunk_len))
                .await?;
        session.set_chunk_ref(array.clone(), index, Some(payload)).await?;
        Ok(session)
    }
}

/// Opens the repository, retrying until it opens or the worker should stop.
async fn open_until_stopped(
    root: &RunRoot,
    config: &RunConfig,
    hooks: &Arc<dyn CasHooks>,
    stop: &mut StopCheck<'_>,
) -> Option<Repository> {
    while !stop.stopped() {
        match open_repo(root, config, Some(Arc::clone(hooks))).await {
            Ok(repo) => return Some(repo),
            Err(err) => {
                tracing::warn!("{err:#}");
                tokio::time::sleep(ERROR_BACKOFF).await;
            }
        }
    }
    None
}

/// Runs one repository-mode worker until `until` or a `stop` control:
/// 60 % branch commits, 30 % `main` commits, 10 % tags. Repository errors
/// are logged and retried; only failing to write our own log is fatal.
pub async fn worker(
    root: &RunRoot,
    config: &RunConfig,
    who: &WorkerId,
    until: SystemTime,
) -> anyhow::Result<()> {
    let log = Arc::new(start_worker_log(root, who)?);
    let hooks: Arc<dyn CasHooks> =
        Arc::new(FaultHooks::new(&config.faults, config.sabotage, Arc::clone(&log)));
    let mut stop = StopCheck::new(root, until);
    let Some(repo) = open_until_stopped(root, config, &hooks, &mut stop).await else {
        return Ok(());
    };
    let mut worker = RepoWorker::new(repo, log, who, config.chunk_bytes)?;
    loop {
        if stop.stopped() {
            return Ok(());
        }
        if worker.ensure_branch().await? {
            break;
        }
        tokio::time::sleep(ERROR_BACKOFF).await;
    }
    let mut rng: StdRng = rand::make_rng();
    while !stop.stopped() {
        let r: f64 = rng.random();
        let result = if r < BRANCH_COMMIT_SHARE {
            worker.branch_commit().await?
        } else if r < BRANCH_COMMIT_SHARE + MAIN_COMMIT_SHARE {
            worker.main_commit().await?
        } else {
            worker.tag().await?
        };
        if matches!(result, Some(OpResult::Error { .. })) {
            tokio::time::sleep(ERROR_BACKOFF).await;
        }
    }
    Ok(())
}

const GC_TICK: Duration = Duration::from_millis(100);

/// The GC worker: every `gc.every`, garbage collects what is older than
/// `gc.margin`, with fault hooks like any worker.
pub async fn gc_worker(
    root: &RunRoot,
    config: &RunConfig,
    who: &WorkerId,
    until: SystemTime,
) -> anyhow::Result<()> {
    let gc = config.gc.context("the run has no gc configuration")?;
    let margin = TimeDelta::from_std(gc.margin).context("gc.margin is too long")?;
    let log = Arc::new(start_worker_log(root, who)?);
    let hooks: Arc<dyn CasHooks> =
        Arc::new(FaultHooks::new(&config.faults, config.sabotage, Arc::clone(&log)));
    let mut stop = StopCheck::new(root, until);
    let Some(repo) = open_until_stopped(root, config, &hooks, &mut stop).await else {
        return Ok(());
    };
    let mut next = Instant::now();
    while !stop.stopped() {
        let now = Instant::now();
        if now < next {
            tokio::time::sleep(GC_TICK.min(next - now)).await;
            continue;
        }
        next = now + gc.every;
        let cutoff = Utc::now() - margin;
        let id = Uuid::new_v4();
        log.append(&Record::Intent { t_ms: now_ms(), id, op: Op::Gc })?;
        let result = match collect_garbage(&repo, config, cutoff).await {
            Ok(_) => OpResult::Ack { snapshot: None },
            Err(err) => OpResult::Error { message: err.to_string() },
        };
        log.append(&Record::Outcome { t_ms: now_ms(), id, result })?;
    }
    Ok(())
}

/// Garbage collects everything unreachable and older than `cutoff`, and the
/// storage's leftovers.
pub async fn collect_garbage(
    repo: &Repository,
    config: &RunConfig,
    cutoff: chrono::DateTime<Utc>,
) -> GCResult<GCSummary> {
    garbage_collect(
        Arc::clone(repo.asset_manager()),
        &gc_config(cutoff),
        Some(&repo_update_retries(config)),
        repo.config().num_updates_per_repo_info_file(),
    )
    .await
}

fn gc_config(cutoff: chrono::DateTime<Utc>) -> GCConfig {
    let u16_limit = |n| NonZeroU16::new(n).unwrap_or(NonZeroU16::MIN);
    let bytes_limit = |n| NonZeroUsize::new(n).unwrap_or(NonZeroUsize::MIN);
    GCConfig::clean_all(
        cutoff,
        cutoff,
        None,
        u16_limit(50),
        bytes_limit(512 * 1024 * 1024),
        bytes_limit(4 * 1024 * 1024 * 1024),
        u16_limit(500),
        u16_limit(10),
        u16_limit(50),
        None,
        false,
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeSet;

    use icechunk::format::SnapshotId;

    use super::*;
    use crate::{
        config::{Backend, FaultConfig, Mode, PointName},
        control::{Control, write_control},
        log::{FaultAction, read_all_logs, read_log},
        oracle::{History, Verdict, repo::check_repo},
    };

    /// An initialized repository-mode run root without faults.
    pub(crate) async fn repo_root() -> (tempfile::TempDir, RunRoot, RunConfig) {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().join("run"));
        root.create_dirs().unwrap();
        let mut config = RunConfig::defaults(Mode::Repository, 1);
        config.faults.points = vec![];
        init(&root, &config).await.unwrap();
        (dir, root, config)
    }

    pub(crate) fn who(tag: &str, slot: &str, incarnation: u32) -> WorkerId {
        WorkerId { tag: tag.to_owned(), slot: slot.to_owned(), incarnation }
    }

    /// A worker as `worker` builds it, with the fault hooks installed.
    pub(crate) async fn repo_worker(
        root: &RunRoot,
        config: &RunConfig,
        who: &WorkerId,
    ) -> RepoWorker {
        let log = Arc::new(start_worker_log(root, who).unwrap());
        let hooks = FaultHooks::new(&config.faults, config.sabotage, Arc::clone(&log));
        let repo = open_repo(root, config, Some(Arc::new(hooks))).await.unwrap();
        RepoWorker::new(repo, log, who, config.chunk_bytes).unwrap()
    }

    fn acked_snapshot(result: Option<OpResult>) -> String {
        match result {
            Some(OpResult::Ack { snapshot: Some(snapshot) }) => snapshot,
            other => panic!("expected an ack with a snapshot, got {other:?}"),
        }
    }

    #[test]
    fn commit_messages_round_trip() {
        let id = Uuid::new_v4();
        assert_eq!(parse_commit_message(&commit_message(id)), Some(id));
        let simple = id.simple().to_string();
        let upper = id.hyphenated().to_string().to_uppercase();
        for bad in [
            "Repository initialized".to_owned(),
            format!("stress  {id}"),
            format!("stress {simple}"),
            format!("stress {upper}"),
            format!("stress {id} "),
            format!("{id}"),
        ] {
            assert_eq!(parse_commit_message(&bad), None, "{bad:?}");
        }
    }

    #[test]
    fn names_follow_the_layout() {
        let id = Uuid::new_v4();
        assert_eq!(branch_name("h.x-1", "3"), "w.h.x-1.3");
        assert_eq!(parse_branch_name("w.h.x-1.3"), Some(("h.x-1", "3")));
        assert_eq!(parse_branch_name("w.h.gc"), Some(("h", "gc")));
        for bad in ["main", "w.h", "w..3", "w.h.", "x.h.3", "w.h space.3"] {
            assert_eq!(parse_branch_name(bad), None, "{bad:?}");
        }
        assert_eq!(array_path_on_branch("h.x", "3").unwrap().to_string(), "/w.h.x.3");
        assert_eq!(array_path_on_main("h.x", "3").unwrap().to_string(), "/m.h.x.3");
        assert_eq!(tag_name(id), format!("t-{id}"));
    }

    #[test]
    fn chunk_bytes_repeat_the_id() {
        let id = Uuid::new_v4();
        let text = id.hyphenated().to_string();
        assert_eq!(chunk_bytes(id, 0), Bytes::new());
        assert_eq!(chunk_bytes(id, 5), Bytes::from(text[..5].to_owned()));
        let long = chunk_bytes(id, 100);
        assert_eq!(long.len(), 100);
        assert_eq!(&long[..36], text.as_bytes());
        assert_eq!(&long[36..72], text.as_bytes());
        assert_eq!(&long[72..], &text.as_bytes()[..28]);
    }

    #[test]
    fn classify_maps_conflict_kinds() {
        let session = |kind| classify(&SessionError::capture(kind));
        let repository = |kind| classify_repository(&RepositoryError::capture(kind));
        let snapshot = SnapshotId::random();
        assert_eq!(
            session(SessionErrorKind::Conflict {
                expected_parent: Some(snapshot.clone()),
                actual_parent: None,
            }),
            OpResult::Conflict
        );
        assert_eq!(
            session(SessionErrorKind::RebaseFailed { snapshot, conflicts: vec![] }),
            OpResult::Conflict
        );
        assert_eq!(
            session(SessionErrorKind::RepositoryError(
                RepositoryErrorKind::RepoUpdateAttemptsLimit(3)
            )),
            OpResult::Conflict
        );
        assert_eq!(
            repository(RepositoryErrorKind::RepoUpdateAttemptsLimit(3)),
            OpResult::Conflict
        );
        assert_eq!(
            repository(RepositoryErrorKind::Ref(RefErrorKind::TagAlreadyExists(
                "t-x".to_owned()
            ))),
            OpResult::Conflict
        );

        let error = session(SessionErrorKind::ReadOnlySession);
        assert!(
            matches!(&error, OpResult::Error { message }
                if *message == SessionErrorKind::ReadOnlySession.to_string()),
            "{error:?}"
        );
        assert!(matches!(
            repository(RepositoryErrorKind::Ref(RefErrorKind::RefNotFound(
                "b".to_owned()
            ))),
            OpResult::Error { .. }
        ));
    }

    #[tokio::test]
    async fn names_are_accepted_by_icechunk() {
        let (_dir, root, config) = repo_root().await;
        let who = who("a.b_c-9", "0", 0);
        let mut worker = repo_worker(&root, &config, &who).await;
        assert!(worker.ensure_branch().await.unwrap());
        let snapshot = acked_snapshot(worker.branch_commit().await.unwrap());
        acked_snapshot(worker.main_commit().await.unwrap());
        assert_eq!(acked_snapshot(worker.tag().await.unwrap()), snapshot);

        let repo = open_repo(&root, &config, None).await.unwrap();
        let branch = branch_name(&who.tag, &who.slot);
        assert_eq!(
            repo.list_branches().await.unwrap(),
            BTreeSet::from([MAIN_BRANCH.to_owned(), branch.clone()])
        );
        assert_eq!(repo.lookup_branch(&branch).await.unwrap().to_string(), snapshot);
        let tags = repo.list_tags().await.unwrap();
        assert_eq!(tags.len(), 1);
        assert!(tags.iter().all(|tag| tag.starts_with("t-")), "{tags:?}");
    }

    #[tokio::test]
    async fn the_legacy_backend_commits_and_tags() {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().join("run"));
        root.create_dirs().unwrap();
        let mut config = RunConfig::defaults(Mode::Repository, 1);
        config.backend = Backend::LegacyLocal;
        config.faults.kill_probability = 0.0;
        config.faults.pause_probability = 0.0;
        config.validate().unwrap();
        init(&root, &config).await.unwrap();
        let who = who("h", "0", 0);
        let mut worker = repo_worker(&root, &config, &who).await;
        assert!(worker.ensure_branch().await.unwrap());
        let snapshot = acked_snapshot(worker.branch_commit().await.unwrap());
        acked_snapshot(worker.main_commit().await.unwrap());
        assert_eq!(acked_snapshot(worker.tag().await.unwrap()), snapshot);

        let repo = open_repo(&root, &config, None).await.unwrap();
        let branch = branch_name(&who.tag, &who.slot);
        assert_eq!(repo.lookup_branch(&branch).await.unwrap().to_string(), snapshot);
        assert!(root.storage().join(storage_mode::REPO_KEY).is_file());

        let history =
            History { logs: read_all_logs(&root).unwrap(), ..History::default() };
        let mut verdict = Verdict::default();
        check_repo(&repo, &history, &config, false, &mut verdict).await.unwrap();
        assert_eq!(verdict.violations, [], "{verdict:?}");
    }

    #[tokio::test]
    async fn ensure_branch_logs_one_create_across_incarnations() {
        let (_dir, root, config) = repo_root().await;
        let first = repo_worker(&root, &config, &who("h", "0", 0)).await;
        assert!(first.ensure_branch().await.unwrap());
        let second = repo_worker(&root, &config, &who("h", "0", 1)).await;
        assert!(second.ensure_branch().await.unwrap());

        let creates = |incarnation| {
            read_log(&root.worker_log("h", "0", incarnation))
                .unwrap()
                .into_iter()
                .filter(|record| {
                    matches!(record, Record::Intent { op: Op::CreateBranch { .. }, .. })
                })
                .count()
        };
        assert_eq!((creates(0), creates(1)), (1, 0));
    }

    #[tokio::test]
    async fn tag_needs_an_acked_branch_commit() {
        let (_dir, root, config) = repo_root().await;
        let worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
        assert_eq!(worker.tag().await.unwrap(), None);
    }

    #[tokio::test]
    async fn fault_hooks_fire_in_repository_mode() {
        let (_dir, root, mut config) = repo_root().await;
        config.faults = FaultConfig {
            points: vec![PointName::Compared],
            kill_probability: 0.0,
            pause_probability: 1.0,
            pause: Duration::from_millis(1),
        };
        let who = who("h", "0", 0);
        let mut worker = repo_worker(&root, &config, &who).await;
        assert!(worker.ensure_branch().await.unwrap());
        acked_snapshot(worker.branch_commit().await.unwrap());
        let records = read_log(&root.worker_log("h", "0", 0)).unwrap();
        assert!(
            records.iter().any(|record| matches!(
                record,
                Record::Fault {
                    action: FaultAction::Pause,
                    point: PointName::Compared,
                    ..
                }
            )),
            "{records:?}"
        );
    }

    #[tokio::test]
    async fn stop_control_ends_both_workers() {
        let (_dir, root, mut config) = repo_root().await;
        config.gc = Some(crate::config::GcConfig::defaults(config.faults.pause));
        write_control(&root, &Control::Stop).unwrap();
        let until = SystemTime::now() + Duration::from_secs(600);
        let ten_seconds = Duration::from_secs(10);
        tokio::time::timeout(
            ten_seconds,
            worker(&root, &config, &who("h", "0", 0), until),
        )
        .await
        .unwrap()
        .unwrap();
        tokio::time::timeout(
            ten_seconds,
            gc_worker(&root, &config, &who("h", "gc", 0), until),
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn gc_worker_logs_acked_collections() {
        let (_dir, root, mut config) = repo_root().await;
        config.gc = Some(crate::config::GcConfig {
            every: Duration::from_millis(10),
            margin: Duration::from_secs(60),
        });
        let until = SystemTime::now() + Duration::from_secs(600);
        let who = who("h", "gc", 0);
        let gc = tokio::spawn({
            let (root, config, who) = (root.clone(), config.clone(), who.clone());
            async move { gc_worker(&root, &config, &who, until).await }
        });
        let log = root.worker_log(&who.tag, &who.slot, who.incarnation);
        let is_ack = |record: &Record| {
            matches!(record, Record::Outcome { result: OpResult::Ack { .. }, .. })
        };
        let run = async {
            while !read_log(&log).is_ok_and(|records| records.iter().any(is_ack)) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            write_control(&root, &Control::Stop).unwrap();
            gc.await.unwrap()
        };
        tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .expect("the GC worker didn't ack within 60 s")
            .unwrap();
        let records = read_log(&log).unwrap();
        let acks = records.iter().filter(|record| is_ack(record)).count();
        let intents = records
            .iter()
            .filter(|record| matches!(record, Record::Intent { op: Op::Gc, .. }))
            .count();
        assert!(acks >= 1 && acks == intents, "{records:?}");
    }
}
