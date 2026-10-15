//! Storage mode: workers race conditional updates of `repo` directly through
//! the [`Storage`] API. Before each attempt a worker backs up the base it read
//! to `overwritten/<attempt id>`, so the oracle can rebuild the chain of
//! committed payloads from the backups.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{Context as _, bail};
use bytes::Bytes;
use icechunk::{
    asset_manager::async_reader_to_bytes,
    storage::{
        GetModifiedResult, PutCondition, Settings, Storage, StorageContext,
        StorageResult, VersionInfo, VersionedUpdateResult, WriteHint,
        new_local_filesystem_storage,
    },
};
use icechunk_fs::{FilesystemStorage, fault_injection::ProtocolHooks};
use rand::{Rng, RngExt as _, rngs::StdRng};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    config::{Backend, RunConfig, WorkerId},
    control::StopCheck,
    faults::FaultHooks,
    layout::RunRoot,
    log::{LogWriter, Op, OpResult, Record, now_ms, start_worker_log},
};

pub const REPO_KEY: &str = "repo";

/// Where workers back up the base of each attempt.
pub const BACKUP_DIR: &str = "overwritten";

pub fn backup_key(attempt: Uuid) -> String {
    format!("{BACKUP_DIR}/{attempt}")
}

/// The full content of `repo` and of every backup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Payload {
    pub id: Uuid,
    /// The etag of the payload this one replaced; `None` only initially.
    pub parent: Option<String>,
    pub host: String,
    pub slot: String,
    pub incarnation: u32,
    pub seq: u64,
    pub padding: String,
}

/// The etag of a payload: the lowercase sha256 hex of its bytes, which is
/// also the etag `FilesystemStorage` gives them: the `h` of `repo.d/<N>.<h>`.
pub fn payload_etag(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn parse_payload(bytes: &[u8]) -> anyhow::Result<Payload> {
    serde_json::from_slice(bytes).context("parsing payload")
}

/// The run's storage. `hooks` are installed on the filesystem backend only;
/// [`RunConfig::validate`] rules out a legacy-local run whose hooks would
/// ever fire.
pub async fn storage(
    root: &RunRoot,
    config: &RunConfig,
    hooks: Option<Arc<dyn ProtocolHooks>>,
) -> anyhow::Result<Arc<dyn Storage + Send + Sync>> {
    match config.backend {
        Backend::Filesystem => {
            let storage =
                FilesystemStorage::new(&root.storage())?.with_fsync(config.fsync);
            Ok(Arc::new(match hooks {
                Some(hooks) => storage.with_protocol_hooks(hooks),
                None => storage,
            }))
        }
        Backend::LegacyLocal => Ok(new_local_filesystem_storage(&root.storage()).await?),
    }
}

/// The current content of `repo`, read through `storage`: with
/// `FilesystemStorage` the file `repo` is only a stub.
pub async fn read_repo(storage: &(dyn Storage + Send + Sync)) -> anyhow::Result<Bytes> {
    let settings = storage.default_settings().await?;
    let ctx = StorageContext::unattributed(&settings);
    let reader = storage.get_object(&ctx, REPO_KEY, None).await?;
    Ok(async_reader_to_bytes(reader, 0).await?)
}

/// Creates `repo` with the initial payload, the root of every chain.
pub async fn init(root: &RunRoot, config: &RunConfig) -> anyhow::Result<()> {
    let storage = storage(root, config, None).await?;
    let settings = storage.default_settings().await?;
    let ctx = StorageContext::unattributed(&settings);
    storage.create_location_if_needed().await?;
    let initial = Payload {
        id: Uuid::nil(),
        parent: None,
        host: String::new(),
        slot: String::new(),
        incarnation: 0,
        seq: 0,
        padding: padding(config),
    };
    let bytes = serde_json::to_vec(&initial)?;
    match storage
        .put_object_conditional(
            &ctx,
            REPO_KEY,
            bytes.into(),
            None,
            Vec::new(),
            PutCondition::Create,
        )
        .await?
    {
        VersionedUpdateResult::Updated { .. } => Ok(()),
        VersionedUpdateResult::NotOnLatestVersion => {
            bail!("a repository already exists in {}", root.storage().display())
        }
    }
}

fn padding(config: &RunConfig) -> String {
    "x".repeat(config.payload_bytes)
}

const ERROR_BACKOFF: Duration = Duration::from_millis(100);
const CONFLICT_BACKOFF_MIN: Duration = Duration::from_millis(1);
const CONFLICT_BACKOFF_MAX: Duration = Duration::from_millis(10);

/// How long to wait after losing a race. Without it, losers on a fast
/// filesystem retry within microseconds, and each attempt writes a backup
/// and two log records.
fn conflict_backoff(rng: &mut impl Rng) -> Duration {
    rng.random_range(CONFLICT_BACKOFF_MIN..=CONFLICT_BACKOFF_MAX)
}

/// Updates `repo` in a loop until `until` or a `stop` control. Storage errors
/// are logged and retried; only failing to write our own log is fatal.
pub async fn worker(
    root: &RunRoot,
    config: &RunConfig,
    who: &WorkerId,
    until: SystemTime,
) -> anyhow::Result<()> {
    let log = Arc::new(start_worker_log(root, who)?);
    let hooks = FaultHooks::new(&config.faults, config.sabotage, Arc::clone(&log));
    let storage = storage(root, config, Some(Arc::new(hooks))).await?;
    let mut updater = Updater::new(storage, who, config).await?;
    let mut stop = StopCheck::new(root, until);
    let mut rng: StdRng = rand::make_rng();
    while !stop.stopped() {
        let backoff = match updater.attempt(&log).await? {
            Attempt::Raced | Attempt::Logged(OpResult::Conflict) => {
                conflict_backoff(&mut rng)
            }
            Attempt::Failed(err) => {
                tracing::warn!("{err}");
                log.append(&Record::Failure { t_ms: now_ms(), message: err })?;
                ERROR_BACKOFF
            }
            Attempt::Logged(OpResult::Error { .. }) => ERROR_BACKOFF,
            Attempt::Logged(OpResult::Ack { .. }) => continue,
        };
        tokio::time::sleep(backoff).await;
    }
    Ok(())
}

/// What one [`Updater::attempt`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attempt {
    /// `repo` changed while it was backed up; nothing was logged.
    Raced,
    /// Reading or backing up `repo` failed; no intent was logged.
    Failed(String),
    /// The update's intent and this outcome were logged.
    Logged(OpResult),
}

/// One writer's conditional updates of `repo`, each on top of the version it
/// last saw.
#[derive(Debug)]
pub struct Updater {
    storage: Arc<dyn Storage + Send + Sync>,
    settings: Settings,
    backend: Backend,
    who: WorkerId,
    padding: String,
    cached: Option<Base>,
    seq: u64,
}

/// A version of `repo` with its bytes and their [`payload_etag`].
#[derive(Debug, Clone)]
struct Base {
    version: VersionInfo,
    etag: String,
    bytes: Bytes,
}

impl Updater {
    /// Writes with the storage's own default settings, as icechunk does.
    pub async fn new(
        storage: Arc<dyn Storage + Send + Sync>,
        who: &WorkerId,
        config: &RunConfig,
    ) -> anyhow::Result<Self> {
        let settings = storage.default_settings().await?;
        Ok(Self {
            storage,
            settings,
            backend: config.backend,
            who: who.clone(),
            padding: padding(config),
            cached: None,
            seq: 0,
        })
    }

    /// Backs up the current `repo`, logs an intent and tries to replace
    /// `repo` with a new payload. `Err` only if `log` can't be written.
    pub async fn attempt(&mut self, log: &LogWriter) -> anyhow::Result<Attempt> {
        let ctx = StorageContext::unattributed(&self.settings);
        let storage = &*self.storage;
        let base = match latest(storage, &ctx, self.cached.take(), self.backend).await {
            Ok(base) => base,
            Err(err) => {
                return Ok(Attempt::Failed(format!("reading {REPO_KEY}: {err:#}")));
            }
        };
        let id = Uuid::new_v4();
        let payload = Payload {
            id,
            parent: Some(base.etag.clone()),
            host: self.who.tag.clone(),
            slot: self.who.slot.clone(),
            incarnation: self.who.incarnation,
            seq: self.seq,
            padding: self.padding.clone(),
        };
        let bytes = Bytes::from(serde_json::to_vec(&payload)?);
        let new = payload_etag(&bytes);

        match back_up(storage, &ctx, self.backend, id, &base).await {
            Ok(true) => {}
            Ok(false) => return Ok(Attempt::Raced),
            Err(err) => {
                return Ok(Attempt::Failed(format!("backing up {REPO_KEY}: {err}")));
            }
        }

        log.append(&Record::Intent {
            t_ms: now_ms(),
            id,
            op: Op::Cas { base: base.etag, new: new.clone() },
        })?;
        let put = storage
            .put_object_conditional(
                &ctx,
                REPO_KEY,
                bytes.clone(),
                None,
                Vec::new(),
                PutCondition::IfVersion(&base.version),
            )
            .await;
        let result = match put {
            Ok(VersionedUpdateResult::Updated { new_version }) => {
                self.cached = Some(Base { version: new_version, etag: new, bytes });
                OpResult::Ack { snapshot: None }
            }
            Ok(VersionedUpdateResult::NotOnLatestVersion) => OpResult::Conflict,
            Err(err) => OpResult::Error { message: err.to_string() },
        };
        log.append(&Record::Outcome { t_ms: now_ms(), id, result: result.clone() })?;
        self.seq += 1;
        Ok(Attempt::Logged(result))
    }
}

/// `repo` as it is now: `cached` if it still is, otherwise read afresh.
async fn latest(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    cached: Option<Base>,
    backend: Backend,
) -> anyhow::Result<Base> {
    let previous = cached.as_ref().map(|base| &base.version);
    match storage.get_object_conditional(ctx, REPO_KEY, previous).await? {
        GetModifiedResult::Modified { data, new_version } => {
            let bytes = async_reader_to_bytes(data, 0).await?;
            let etag = payload_etag(&bytes);
            // The version we update must be the content we read. Only
            // `FilesystemStorage` derives its etags from the content, and it
            // checks them on every read, so this holds by construction.
            if backend == Backend::Filesystem && new_version.etag() != Some(&etag) {
                bail!(
                    "read {REPO_KEY} with content hashing to {etag} as {new_version:?}"
                );
            }
            Ok(Base { version: new_version, etag, bytes })
        }
        GetModifiedResult::OnLatestVersion => {
            cached.context("storage reported an unchanged version without one to compare")
        }
    }
}

/// Backs up `base` as the base of attempt `id`; `false` if `repo` changed
/// first.
async fn back_up(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    backend: Backend,
    id: Uuid,
    base: &Base,
) -> StorageResult<bool> {
    let key = backup_key(id);
    match backend {
        Backend::Filesystem => {
            let copied =
                storage.copy_object(ctx, REPO_KEY, &key, None, &base.version).await?;
            Ok(matches!(copied, VersionedUpdateResult::Updated { .. }))
        }
        // The legacy copy is unconditional, so it could back up a newer
        // `repo` than the one read and leave the chain without this base.
        Backend::LegacyLocal => {
            let bytes = base.bytes.clone();
            storage
                .put_object(ctx, &key, bytes, None, Vec::new(), WriteHint::Atomic)
                .await?;
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        fs,
    };

    use super::*;
    use crate::{
        config::{Backend, Mode},
        control::{Control, write_control},
        log::{read_all_logs, read_log},
        oracle::{
            History, Verdict,
            storage::{check_chain, read_backups},
        },
    };

    fn run_root() -> (tempfile::TempDir, RunRoot, RunConfig) {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().to_path_buf());
        fs::create_dir_all(root.logs_dir()).unwrap();
        let mut config = RunConfig::defaults(Mode::Storage, 1);
        config.faults.points = vec![];
        (dir, root, config)
    }

    fn who(slot: &str) -> WorkerId {
        WorkerId { tag: "h".to_owned(), slot: slot.to_owned(), incarnation: 0 }
    }

    /// Checks the log is `Start`, then intents each followed by its outcome
    /// (ignoring `Observed` records), and returns the `new` etag of every acknowledged update.
    fn acked(root: &RunRoot, who: &WorkerId) -> Vec<String> {
        let records =
            read_log(&root.worker_log(&who.tag, &who.slot, who.incarnation)).unwrap();
        let (start, ops) = records.split_first().unwrap();
        assert!(
            matches!(start, Record::Start { host, slot, incarnation: 0, pid, .. }
                if *host == who.tag && *slot == who.slot && *pid == std::process::id()),
            "{start:?}"
        );
        let ops: Vec<_> = ops
            .iter()
            .filter(|record| !matches!(record, Record::Observed { .. }))
            .cloned()
            .collect();
        assert_eq!(ops.len() % 2, 0, "{ops:?}");
        ops.chunks(2)
            .filter_map(|pair| match pair {
                [
                    Record::Intent { id, op: Op::Cas { base, new }, .. },
                    Record::Outcome { id: outcome_id, result, .. },
                ] => {
                    assert_eq!(id, outcome_id);
                    assert_ne!(base, new);
                    match result {
                        OpResult::Ack { snapshot: None } => Some(new.clone()),
                        OpResult::Conflict => None,
                        other => panic!("unexpected outcome {other:?}"),
                    }
                }
                other => panic!("expected intent then outcome, got {other:?}"),
            })
            .collect()
    }

    async fn repo_bytes(root: &RunRoot, config: &RunConfig) -> Bytes {
        read_repo(&*storage(root, config, None).await.unwrap()).await.unwrap()
    }

    /// Follows `repo`'s parents through the backups down to the initial
    /// payload and returns the etags of the whole chain.
    async fn chain(root: &RunRoot, config: &RunConfig) -> HashSet<String> {
        let storage = root.storage();
        let backups: HashMap<String, Payload> = fs::read_dir(storage.join("overwritten"))
            .unwrap()
            .map(|entry| {
                let bytes = fs::read(entry.unwrap().path()).unwrap();
                (payload_etag(&bytes), parse_payload(&bytes).unwrap())
            })
            .collect();
        let bytes = repo_bytes(root, config).await;
        let mut etags = HashSet::from([payload_etag(&bytes)]);
        let mut payload = parse_payload(&bytes).unwrap();
        while let Some(parent) = &payload.parent {
            assert!(etags.insert(parent.clone()), "cycle at {parent}");
            payload = backups
                .get(parent)
                .unwrap_or_else(|| panic!("no backup of {parent}"))
                .clone();
        }
        assert_eq!(payload.id, Uuid::nil());
        etags
    }

    fn has_ack(root: &RunRoot, who: &WorkerId) -> bool {
        // The log may not exist yet.
        read_log(&root.worker_log(&who.tag, &who.slot, who.incarnation)).is_ok_and(
            |records| {
                records.iter().any(|record| {
                    matches!(record, Record::Outcome { result: OpResult::Ack { .. }, .. })
                })
            },
        )
    }

    /// Runs one worker per slot until every one has an acknowledged update,
    /// then stops them through `control`. The protocol has no fairness,
    /// so a fixed time budget could leave a slot without a single win.
    async fn run_until_each_acks(root: &RunRoot, config: &RunConfig, slots: &[&str]) {
        init(root, config).await.unwrap();
        let until = SystemTime::now() + Duration::from_secs(600);
        let mut workers = tokio::task::JoinSet::new();
        for slot in slots {
            let (root, config, who) = (root.clone(), config.clone(), who(slot));
            workers.spawn(async move { worker(&root, &config, &who, until).await });
        }
        let run = async {
            while !slots.iter().all(|slot| has_ack(root, &who(slot))) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            write_control(root, &Control::Stop).unwrap();
            workers.join_all().await
        };
        let results = tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .unwrap_or_else(|_| panic!("workers {slots:?} didn't all ack within 60 s"));
        for result in results {
            result.unwrap();
        }
    }

    #[tokio::test]
    async fn one_worker_builds_a_chain_covered_by_backups() {
        let (_dir, root, config) = run_root();
        run_until_each_acks(&root, &config, &["0"]).await;
        let acked = acked(&root, &who("0"));
        assert!(!acked.is_empty());
        let chain = chain(&root, &config).await;
        assert!(acked.iter().all(|etag| chain.contains(etag)));
    }

    #[tokio::test]
    async fn one_worker_on_the_legacy_backend_builds_a_valid_chain() {
        let (_dir, root, mut config) = run_root();
        config.backend = Backend::LegacyLocal;
        config.faults.kill_probability = 0.0;
        config.faults.pause_probability = 0.0;
        config.validate().unwrap();
        run_until_each_acks(&root, &config, &["0"]).await;
        assert!(!acked(&root, &who("0")).is_empty());

        let final_repo = repo_bytes(&root, &config).await;
        let backups = read_backups(&root.storage()).unwrap();
        let history =
            History { logs: read_all_logs(&root).unwrap(), ..History::default() };
        let mut verdict = Verdict::default();
        check_chain(Some(&final_repo), &backups, &history, false, &mut verdict);
        assert_eq!(verdict.violations, [], "{verdict:?}");
        assert!(verdict.stats.acks > 0 && verdict.stats.chain_len > 1, "{verdict:?}");
    }

    #[tokio::test]
    async fn two_workers_both_commit() {
        let (_dir, root, config) = run_root();
        run_until_each_acks(&root, &config, &["0", "1"]).await;
        let chain = chain(&root, &config).await;
        for slot in ["0", "1"] {
            let acked = acked(&root, &who(slot));
            assert!(!acked.is_empty(), "slot {slot}");
            assert!(acked.iter().all(|etag| chain.contains(etag)));
        }
    }

    #[tokio::test]
    async fn init_refuses_an_existing_repo() {
        let (_dir, root, config) = run_root();
        init(&root, &config).await.unwrap();
        let other =
            RunConfig { payload_bytes: config.payload_bytes + 1, ..config.clone() };
        assert!(init(&root, &other).await.is_err());
        let payload = parse_payload(&repo_bytes(&root, &config).await).unwrap();
        assert_eq!(payload.id, Uuid::nil());
        assert_eq!(payload.parent, None);
        assert_eq!(payload.padding, "x".repeat(config.payload_bytes));
    }

    #[test]
    fn conflict_backoff_is_jittered_within_bounds() {
        use rand::SeedableRng as _;
        let mut rng = StdRng::seed_from_u64(7);
        let draws: Vec<_> = (0..10_000).map(|_| conflict_backoff(&mut rng)).collect();
        let range = Duration::from_millis(1)..=Duration::from_millis(10);
        assert!(draws.iter().all(|draw| range.contains(draw)), "{range:?}");
        assert!(draws.iter().any(|draw| *draw < Duration::from_millis(2)));
        assert!(draws.iter().any(|draw| *draw > Duration::from_millis(9)));
    }

    #[tokio::test]
    async fn stop_control_ends_the_worker() {
        let (_dir, root, config) = run_root();
        init(&root, &config).await.unwrap();
        write_control(&root, &Control::Stop).unwrap();
        let until = SystemTime::now() + Duration::from_secs(600);
        tokio::time::timeout(
            Duration::from_secs(10),
            worker(&root, &config, &who("0"), until),
        )
        .await
        .unwrap()
        .unwrap();
    }
}
