//! Storage mode: workers race conditional updates of `repo` directly through
//! the [`Storage`] API. Each update is a real repo info: its base with a
//! `MetadataChangedUpdate` whose stress metadata names the writer and the
//! base, made as icechunk makes a change. Before each attempt a worker backs
//! up the base it read to a spec-named file in `overwritten/`, which the new
//! repo info's ops log names, so the oracle can rebuild the chain of repo
//! infos from the backups.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{Context as _, bail, ensure};
use bytes::Bytes;
use icechunk::{
    asset_manager::async_reader_to_bytes,
    format::{
        IcechunkFormatError, OVERWRITTEN_FILES_PATH,
        format_constants::SpecVersionBin,
        repo_info::{RepoInfo, UpdateType},
        repo_info_file,
        snapshot::{Snapshot, SnapshotInfo, SnapshotProperties},
        writer_features::WriterFeature,
    },
    storage::{
        GetModifiedResult, PutCondition, Settings, Storage, StorageContext,
        StorageErrorKind, StorageResult, VersionInfo, VersionedUpdateResult, WriteHint,
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
pub const BACKUP_DIR: &str = OVERWRITTEN_FILES_PATH;

/// The key of a backup whose spec name, relative to [`BACKUP_DIR`], is
/// `backup_path`.
pub fn backup_key(backup_path: &str) -> String {
    format!("{BACKUP_DIR}/{backup_path}")
}

/// The metadata key of a [`Payload`].
pub const METADATA_KEY: &str = "stress";

/// Ops-log entries per repo info file: few, so that paging the ops log out to
/// `repo_before_updates` is exercised.
pub const NUM_UPDATES_PER_FILE: u16 = 50;

const COMPRESSION_LEVEL: u8 = 3;

/// The stress metadata of every repo info a worker writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Payload {
    pub id: Uuid,
    /// The etag of the repo info this one replaced.
    pub parent: String,
    pub host: String,
    pub slot: String,
    pub incarnation: u32,
    pub seq: u64,
    pub padding: String,
}

/// The etag of a repo info: the lowercase sha256 hex of its bytes, which is
/// also the etag `FilesystemStorage` gives them: the `h` of `repo.d/<N>.<h>`.
pub fn payload_etag(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// A repo info file a storage-mode run wrote.
#[derive(Debug)]
pub struct Decoded {
    pub info: RepoInfo,
    /// `None` for the initial repo info, whose ops log is only its
    /// `RepoInitializedUpdate`.
    pub payload: Option<Payload>,
    /// The backup path the update that made this repo info named on its
    /// predecessor entry, as the filesystem storage's lost-reply check reads
    /// it; `None` for the initial repo info.
    pub attempt: Option<String>,
}

/// Decodes a repo info file a storage-mode run wrote.
pub fn decode(bytes: &[u8]) -> anyhow::Result<Decoded> {
    let info = repo_info_file::decode(bytes).map_err(format_error("decoding"))?;
    let payload = match info
        .metadata()
        .map_err(format_error("reading metadata"))?
        .remove(METADATA_KEY)
    {
        Some(value) => {
            Some(serde_json::from_value(value).context("parsing the stress metadata")?)
        }
        None => {
            ensure!(
                is_initial(&info)?,
                "no {METADATA_KEY:?} metadata in a repo info after the initial one"
            );
            None
        }
    };
    let attempt = attempt(&info)?;
    ensure!(
        payload.is_some() == attempt.is_some(),
        "a repo info with{} stress metadata names {} predecessor backup",
        if payload.is_some() { "" } else { "out" },
        if attempt.is_some() { "a" } else { "no" },
    );
    Ok(Decoded { info, payload, attempt })
}

/// The backup path the update that made `info` wrote on its predecessor
/// entry, read as `layout_v1::attempt_id` reads it from the file, without
/// decoding the file again: when paging moved the predecessor out of this
/// file, the file's `repo_before_updates`.
fn attempt(info: &RepoInfo) -> anyhow::Result<Option<String>> {
    let read = || format_error("reading the attempt");
    let mut entries = info.latest_updates().map_err(read())?;
    // The newest entry is this update's own.
    entries.next().transpose().map_err(read())?;
    Ok(match entries.next().transpose().map_err(read())? {
        Some((_, _, backup_path)) => backup_path.map(str::to_owned),
        None => info.repo_before_updates().map_err(read())?.map(str::to_owned),
    })
}

/// Whether the ops log is only a `RepoInitializedUpdate`.
fn is_initial(info: &RepoInfo) -> anyhow::Result<bool> {
    let mut updates =
        info.latest_updates().map_err(format_error("reading the ops log"))?;
    let first =
        updates.next().transpose().map_err(format_error("reading the ops log"))?;
    let only = matches!(first, Some((UpdateType::RepoInitializedUpdate, ..)))
        && updates.next().is_none()
        && info
            .repo_before_updates()
            .map_err(format_error("reading the ops log"))?
            .is_none();
    Ok(only)
}

/// The initial repo info, listing `writer_features`, as icechunk creates it.
pub fn initial_repo_info(writer_features: &[WriterFeature]) -> anyhow::Result<Bytes> {
    let spec_version = SpecVersionBin::current();
    let snapshot = Snapshot::initial(spec_version)
        .map_err(format_error("building the initial snapshot"))?;
    let snapshot = SnapshotInfo::from_snapshot_file(&snapshot)
        .map_err(format_error("building the initial snapshot"))?;
    let info = RepoInfo::initial(
        spec_version,
        snapshot,
        NUM_UPDATES_PER_FILE,
        None::<&()>,
        None,
        writer_features,
    );
    encode(&info)
}

/// `base` with `payload` as its metadata and its newest ops-log entry naming
/// `backup_path`, as icechunk changes the metadata.
pub fn next_repo_info(
    base: &RepoInfo,
    payload: &Payload,
    backup_path: &str,
) -> anyhow::Result<Bytes> {
    let spec_version =
        base.spec_version().map_err(format_error("reading the spec version"))?;
    let metadata = SnapshotProperties::from([(
        METADATA_KEY.to_owned(),
        serde_json::to_value(payload)?,
    )]);
    let info = base
        .set_metadata(spec_version, &metadata, backup_path, NUM_UPDATES_PER_FILE)
        .map_err(format_error("setting the metadata"))?;
    encode(&info)
}

fn encode(info: &RepoInfo) -> anyhow::Result<Bytes> {
    let spec_version =
        info.spec_version().map_err(format_error("reading the spec version"))?;
    let bytes = repo_info_file::encode(info, spec_version, COMPRESSION_LEVEL)
        .map_err(format_error("encoding"))?;
    Ok(bytes.into())
}

/// Leaves out the error's span trace.
fn format_error(what: &str) -> impl FnOnce(IcechunkFormatError) -> anyhow::Error {
    let what = what.to_owned();
    move |err| anyhow::anyhow!("{what}: {}", err.kind())
}

/// The outcome of a failed storage operation: the layout's refusals and a
/// durable change whose publication found a fork, or an error.
pub fn classify_storage(kind: &StorageErrorKind) -> OpResult {
    let message = kind.to_string();
    match kind {
        StorageErrorKind::RepositoryForked(_) => OpResult::Forked { message },
        StorageErrorKind::RepositoryForkedAfterUpdate { .. } => {
            OpResult::DurableButForked { snapshot: None, message }
        }
        StorageErrorKind::LayoutConversionInProgress(_) => {
            OpResult::ConversionInProgress { message }
        }
        _ => OpResult::Error { message },
    }
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
/// `FilesystemStorage` the file `repo` is the mirror, which can lag.
pub async fn read_repo(storage: &(dyn Storage + Send + Sync)) -> anyhow::Result<Bytes> {
    let settings = storage.default_settings().await?;
    let ctx = StorageContext::unattributed(&settings);
    let reader = storage.get_object(&ctx, REPO_KEY, None).await?;
    Ok(async_reader_to_bytes(reader, 0).await?)
}

/// Creates `repo` with the initial repo info, the root of every chain,
/// listing the writer features the storage requires, as icechunk does.
pub async fn init(root: &RunRoot, config: &RunConfig) -> anyhow::Result<()> {
    let storage = storage(root, config, None).await?;
    let settings = storage.default_settings().await?;
    let ctx = StorageContext::unattributed(&settings);
    storage.create_location_if_needed().await?;
    let required: Vec<WriterFeature> =
        storage.writer_features().await?.into_iter().collect();
    let bytes = initial_repo_info(&required)?;
    match storage
        .put_object_conditional(
            &ctx,
            REPO_KEY,
            bytes,
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

/// Random characters, which compression can't shrink much: repo info files
/// are compressed, and the padding is there to make them bigger.
fn padding(config: &RunConfig) -> String {
    let rng: StdRng = rand::make_rng();
    rng.sample_iter(rand::distr::Alphanumeric)
        .take(config.payload_bytes)
        .map(char::from)
        .collect()
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
            Attempt::Logged(result) if result.landed().is_some() => continue,
            Attempt::Logged(_) => ERROR_BACKOFF,
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
    /// `repo` with a new repo info. `Err` only if `log` can't be written.
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
            parent: base.etag.clone(),
            host: self.who.tag.clone(),
            slot: self.who.slot.clone(),
            incarnation: self.who.incarnation,
            seq: self.seq,
            padding: self.padding.clone(),
        };
        let backup = repo_info_file::new_backup_path();
        let bytes = match repo_info_file::decode(&base.bytes)
            .map_err(format_error("decoding"))
            .and_then(|info| next_repo_info(&info, &payload, &backup))
        {
            Ok(bytes) => bytes,
            Err(err) => {
                return Ok(Attempt::Failed(format!("updating {REPO_KEY}: {err:#}")));
            }
        };
        let new = payload_etag(&bytes);

        match back_up(storage, &ctx, self.backend, &backup, &base).await {
            Ok(true) => {}
            Ok(false) => return Ok(Attempt::Raced),
            Err(err) => {
                return Ok(Attempt::Failed(format!("backing up {REPO_KEY}: {err}")));
            }
        }

        log.append(&Record::Intent {
            t_ms: now_ms(),
            id,
            op: Op::Cas { base: base.etag, new: new.clone(), backup },
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
            Err(err) => classify_storage(err.kind()),
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

/// Backs up `base` to `overwritten/<backup_path>`; `false` if `repo` changed
/// first.
async fn back_up(
    storage: &(dyn Storage + Send + Sync),
    ctx: &StorageContext<'_>,
    backend: Backend,
    backup_path: &str,
    base: &Base,
) -> StorageResult<bool> {
    let key = backup_key(backup_path);
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

    use icechunk::format::layout_v1;

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
    /// (ignoring `Observed` and mirror rename records), and returns the
    /// `new` etag of every acknowledged update.
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
            .filter(|record| {
                !matches!(
                    record,
                    Record::Observed { .. }
                        | Record::MirrorRenaming { .. }
                        | Record::MirrorRename { .. }
                )
            })
            .cloned()
            .collect();
        assert_eq!(ops.len() % 2, 0, "{ops:?}");
        ops.chunks(2)
            .filter_map(|pair| match pair {
                [
                    Record::Intent { id, op: Op::Cas { base, new, .. }, .. },
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

    /// Follows `repo`'s parents through the backups down to the initial repo
    /// info and returns the etags of the whole chain. Each repo info's
    /// attempt names the backup of its parent.
    async fn chain(root: &RunRoot, config: &RunConfig) -> HashSet<String> {
        let overwritten = root.storage().join(BACKUP_DIR);
        // The etag of every backup by name: attempts on the same base back
        // it up under different names.
        let backups: HashMap<String, String> = fs::read_dir(&overwritten)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let bytes = fs::read(entry.path()).unwrap();
                (entry.file_name().into_string().unwrap(), payload_etag(&bytes))
            })
            .collect();
        let backed_up = |attempt: &str| backups.get(attempt);
        let bytes = repo_bytes(root, config).await;
        let mut etags = HashSet::from([payload_etag(&bytes)]);
        let mut decoded = decode(&bytes).unwrap();
        while let Some(payload) = &decoded.payload {
            let parent = payload.parent.clone();
            assert!(etags.insert(parent.clone()), "cycle at {parent}");
            let attempt = decoded.attempt.as_deref().unwrap();
            assert_eq!(backed_up(attempt), Some(&parent), "{attempt}");
            decoded = decode(&fs::read(overwritten.join(attempt)).unwrap()).unwrap();
        }
        assert_eq!(decoded.attempt, None);
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
        let initial = repo_bytes(&root, &config).await;
        assert!(init(&root, &config).await.is_err());
        assert_eq!(repo_bytes(&root, &config).await, initial);
        let decoded = decode(&initial).unwrap();
        assert_eq!((decoded.payload, decoded.attempt), (None, None));
    }

    /// The initial repo info lists the storage's required writer features,
    /// as icechunk's creation does: `filesystem-layout-v1` and its gate on
    /// the filesystem backend, nothing on the legacy one.
    #[tokio::test]
    async fn init_lists_the_writer_features_the_storage_requires() {
        use icechunk::format::repo_info::RepoAvailability;
        for (backend, features) in [
            (Backend::Filesystem, vec![layout_v1::FEATURE]),
            (Backend::LegacyLocal, vec![]),
        ] {
            let (_dir, root, mut config) = run_root();
            config.backend = backend;
            init(&root, &config).await.unwrap();
            let info = decode(&repo_bytes(&root, &config).await).unwrap().info;
            let listed = info.raw_writer_features().unwrap();
            let listed: Vec<&str> = listed.iter().map(|name| name.as_str()).collect();
            assert_eq!(listed, features, "{backend:?}");
            assert_eq!(info.status().unwrap().availability, RepoAvailability::Online);
            info.check_writable_shape(
                icechunk::format::repo_info::WritableShapeMode::AnyChange,
            )
            .unwrap();
        }
    }

    #[test]
    fn next_repo_info_names_the_backup_and_carries_the_payload() {
        let initial = initial_repo_info(&[WriterFeature::FilesystemLayoutV1]).unwrap();
        let mut bytes = initial.clone();
        let mut backups = Vec::new();
        // Past one ops-log file, so the predecessor entry is paged out.
        for seq in 0..u64::from(NUM_UPDATES_PER_FILE) + 2 {
            let payload = Payload {
                id: Uuid::new_v4(),
                parent: payload_etag(&bytes),
                host: "h".to_owned(),
                slot: "0".to_owned(),
                incarnation: 0,
                seq,
                padding: "xx".to_owned(),
            };
            let backup = repo_info_file::new_backup_path();
            let base = repo_info_file::decode(&bytes).unwrap();
            bytes = next_repo_info(&base, &payload, &backup).unwrap();
            let decoded = decode(&bytes).unwrap();
            assert_eq!(decoded.payload, Some(payload));
            assert_eq!(decoded.attempt.as_ref(), Some(&backup));
            // The filesystem storage's lost-reply check reads the same.
            assert_eq!(decoded.attempt, layout_v1::attempt_id(&bytes).unwrap());
            assert!(layout_v1::lists_feature(&bytes).unwrap());
            backups.push(backup);
        }
        let info = decode(&bytes).unwrap().info;
        assert!(info.repo_before_updates().unwrap().is_some());
        assert!(backups.iter().all(|backup| backup.starts_with("repo.")), "{backups:?}");
    }

    /// With one entry per file the predecessor entry is paged out at once,
    /// and the attempt is the file's `repo_before_updates`.
    #[test]
    fn the_attempt_of_a_paged_out_predecessor_is_repo_before_updates() {
        let initial = initial_repo_info(&[WriterFeature::FilesystemLayoutV1]).unwrap();
        let mut info = repo_info_file::decode(&initial).unwrap();
        let spec_version = info.spec_version().unwrap();
        for backup in ["repo.1.A", "repo.2.B"] {
            info = info
                .set_metadata(spec_version, &SnapshotProperties::new(), backup, 1)
                .unwrap();
            let bytes = repo_info_file::encode(&info, spec_version, 3).unwrap();
            assert_eq!(info.latest_updates().unwrap().count(), 1);
            assert_eq!(attempt(&info).unwrap().as_deref(), Some(backup));
            assert_eq!(attempt(&info).unwrap(), layout_v1::attempt_id(&bytes).unwrap());
        }
    }

    #[test]
    fn decode_rejects_repo_infos_a_run_never_writes() {
        let initial = initial_repo_info(&[]).unwrap();
        let base = repo_info_file::decode(&initial).unwrap();
        let spec_version = base.spec_version().unwrap();
        let foreign = SnapshotProperties::from([("other".to_owned(), 1.into())]);
        let foreign = base.set_metadata(spec_version, &foreign, "repo.1.X", 10).unwrap();
        let foreign = repo_info_file::encode(&foreign, spec_version, 3).unwrap();
        let err = format!("{:#}", decode(&foreign).unwrap_err());
        assert!(err.contains("no \"stress\" metadata"), "{err}");

        let odd = SnapshotProperties::from([(METADATA_KEY.to_owned(), 1.into())]);
        let odd = base.set_metadata(spec_version, &odd, "repo.1.X", 10).unwrap();
        let odd = repo_info_file::encode(&odd, spec_version, 3).unwrap();
        assert!(decode(&odd).is_err());
        assert!(decode(b"garbage").is_err());
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
