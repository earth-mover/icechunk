//! The status toggler: a worker slot on the first participant that, every
//! `readonly.every`, sets the repository's status `ReadOnly`, holds it for
//! `readonly.hold`, logs what the mirror and the current version were at the
//! end of the hold (rule M4), then sets the status `Online` again.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use anyhow::Context as _;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use icechunk::{
    Repository, Storage,
    asset_manager::async_reader_to_bytes,
    format::{
        repo_info::{RepoAvailability, RepoStatus, UpdateType},
        repo_info_file,
    },
    storage::{GetModifiedResult, StorageContext},
};
use icechunk_fs::fault_injection::ProtocolHooks;
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::{
    filter::filter_fn,
    layer::{Context, Layer},
    registry::LookupSpan,
};
use uuid::Uuid;

use crate::{
    config::{RunConfig, WorkerId},
    control::StopCheck,
    faults::FaultHooks,
    layout::RunRoot,
    log::{Availability, LogWriter, Op, OpResult, Record, now_ms, start_worker_log},
    repo_mode::{classify_repository, open_until_stopped},
    sampler::sample,
    storage_mode::{self, REPO_KEY},
};

/// The slot of the status toggler.
pub const TOGGLER_SLOT: &str = "toggler";

/// The reason of every `ReadOnly` status the toggler sets.
pub const TOGGLER_REASON: &str = "held by the fs-stress status toggler";

const TICK: Duration = Duration::from_millis(100);
const RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Runs the status toggler until `until` or a `stop` control. A predecessor
/// killed during a hold left the status `ReadOnly`: it sets it `Online`
/// first. Only failing to write its log is fatal.
pub async fn toggler(
    root: &RunRoot,
    config: &RunConfig,
    who: &WorkerId,
    until: SystemTime,
) -> anyhow::Result<()> {
    let readonly = config.readonly.context("the run has no readonly configuration")?;
    let log = Arc::new(start_worker_log(root, who)?);
    // No kills or pauses: a hold judges its own publication.
    let hooks: Arc<dyn ProtocolHooks> =
        Arc::new(FaultHooks::sabotage_only(config.sabotage, Arc::clone(&log)));
    let mut stop = StopCheck::new(root, until);
    let Some(repo) = open_until_stopped(root, config, &hooks, &mut stop).await else {
        return Ok(());
    };
    let storage = storage_mode::storage(root, config, None).await?;
    let toggler = Toggler::new(repo, storage, root.storage().join(REPO_KEY), log);
    let mut next = Instant::now() + readonly.every;
    let mut online = false;
    while !stop.stopped() {
        if !online {
            online = toggler.ensure_online().await?;
            if !online {
                tokio::time::sleep(RETRY_BACKOFF).await;
            }
            continue;
        }
        let now = Instant::now();
        if now < next {
            tokio::time::sleep(TICK.min(next - now)).await;
            continue;
        }
        next = now + readonly.every;
        online = toggler.hold(readonly.hold).await?;
    }
    Ok(())
}

/// The toggler's status `ReadOnly`, with the given `set_at`.
fn read_only(set_at: DateTime<Utc>) -> RepoStatus {
    RepoStatus {
        availability: RepoAvailability::ReadOnly,
        set_at,
        limited_availability_reason: Some(TOGGLER_REASON.to_owned()),
    }
}

/// Errors the storage logged in this process, counted by
/// [`StorageErrorCounter`]: a change whose publication failed returns `Ok`
/// and logs one. The toggler changes the repository alone in its process, so
/// an increase during its change is that change's.
pub static STORAGE_ERRORS: AtomicU64 = AtomicU64::new(0);

/// A tracing layer that counts the error events of `icechunk_fs` in
/// [`STORAGE_ERRORS`].
#[derive(Debug, Clone, Copy, Default)]
pub struct StorageErrorCounter;

impl StorageErrorCounter {
    /// An error event of the `icechunk_fs` crate, not of crates whose names
    /// it prefixes.
    fn counts(metadata: &Metadata<'_>) -> bool {
        let target = metadata.target();
        *metadata.level() == Level::ERROR
            && (target == "icechunk_fs" || target.starts_with("icechunk_fs::"))
    }
}

/// Filters nothing: it must not disable events for the other layers.
/// [`storage_error_counter`] gives it its own filter.
impl<S: Subscriber> Layer<S> for StorageErrorCounter {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if Self::counts(event.metadata()) {
            STORAGE_ERRORS.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// [`StorageErrorCounter`] as a process installs it, next to its other
/// layers: with a filter of its own, which enables its events for it alone.
pub fn storage_error_counter<S>() -> impl Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    StorageErrorCounter.with_filter(filter_fn(StorageErrorCounter::counts))
}

/// An acknowledged `ReadOnly` change of the toggler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOnlySet {
    pub t_ack_ms: i64,
    set_at: DateTime<Utc>,
    /// The storage logged an error during the change.
    pub publish_error: bool,
}

/// Sets and holds the status of one repository.
#[derive(Debug)]
pub struct Toggler {
    repo: Repository,
    /// Without hooks, to read the current version.
    storage: Arc<dyn Storage + Send + Sync>,
    mirror: PathBuf,
    log: Arc<LogWriter>,
    storage_errors: &'static AtomicU64,
}

impl Toggler {
    pub fn new(
        repo: Repository,
        storage: Arc<dyn Storage + Send + Sync>,
        mirror: PathBuf,
        log: Arc<LogWriter>,
    ) -> Self {
        Self { repo, storage, mirror, log, storage_errors: &STORAGE_ERRORS }
    }

    /// Sets the status `Online` unless it is. Whether it is `Online` now;
    /// `Err` only if the log can't be written.
    pub async fn ensure_online(&self) -> anyhow::Result<bool> {
        match self.repo.get_status().await {
            Ok(status) if status.availability == RepoAvailability::Online => Ok(true),
            Ok(_) => self.set(Availability::Online).await,
            Err(err) => {
                tracing::warn!("reading the status: {}", err.kind());
                Ok(false)
            }
        }
    }

    /// Sets the status `ReadOnly`, waits `hold`, logs a
    /// [`Record::ReadOnlyHeld`] if the change was acknowledged, then sets
    /// the status `Online` once. Whether the status is `Online` again; `Err`
    /// only if the log can't be written.
    pub async fn hold(&self, hold: Duration) -> anyhow::Result<bool> {
        if let Some(set) = self.set_read_only().await? {
            match self.own_version(&set).await {
                Ok(version) => {
                    tokio::time::sleep(hold).await;
                    let current = self.current().await.map(|(etag, _)| etag);
                    let mirror = sample(&self.mirror);
                    match current {
                        Ok(current) => self.log.append(&Record::ReadOnlyHeld {
                            t_ack_ms: set.t_ack_ms,
                            version,
                            publish_error: set.publish_error,
                            t_end_ms: now_ms(),
                            current,
                            mirror,
                        })?,
                        Err(err) => tracing::warn!("{err:#}"),
                    }
                }
                Err(err) => tracing::warn!("{err:#}"),
            }
        }
        self.set(Availability::Online).await
    }

    /// Sets the status `ReadOnly`; `None` unless acknowledged. `Err` only if
    /// the log can't be written.
    pub async fn set_read_only(&self) -> anyhow::Result<Option<ReadOnlySet>> {
        let errors = self.storage_errors.load(Ordering::Relaxed);
        // As stored: to the microsecond.
        let set_at = DateTime::from_timestamp_micros(Utc::now().timestamp_micros())
            .unwrap_or_default();
        let acked = self.set_status(Availability::ReadOnly, set_at).await?;
        let publish_error = self.storage_errors.load(Ordering::Relaxed) != errors;
        Ok(acked.then(|| ReadOnlySet { t_ack_ms: now_ms(), set_at, publish_error }))
    }

    /// The etag of the current version if it is the `ReadOnly` change `set`
    /// made: its newest ops-log entry is that status change. `None` if
    /// another change landed after it.
    pub async fn own_version(&self, set: &ReadOnlySet) -> anyhow::Result<Option<String>> {
        let (etag, bytes) = self.current().await?;
        let info = repo_info_file::decode(&bytes).map_err(|err| {
            anyhow::anyhow!("decoding the current version: {}", err.kind())
        })?;
        let newest = info
            .latest_updates()
            .and_then(|mut updates| updates.next().transpose())
            .map_err(|err| {
                anyhow::anyhow!("reading the current version's ops log: {}", err.kind())
            })?;
        let ours = match newest {
            Some((UpdateType::RepoStatusChangedUpdate { status }, ..)) => {
                status.repo_status() == read_only(set.set_at)
            }
            _ => false,
        };
        Ok(ours.then_some(etag))
    }

    async fn set(&self, availability: Availability) -> anyhow::Result<bool> {
        self.set_status(availability, Utc::now()).await
    }

    /// Logs an intent, sets the status and logs the outcome. Whether the
    /// change was acknowledged.
    async fn set_status(
        &self,
        availability: Availability,
        set_at: DateTime<Utc>,
    ) -> anyhow::Result<bool> {
        let id = Uuid::new_v4();
        let op = Op::SetStatus { availability };
        self.log.append(&Record::Intent { t_ms: now_ms(), id, op })?;
        let status = match availability {
            Availability::Online => RepoStatus {
                availability: RepoAvailability::Online,
                set_at,
                limited_availability_reason: None,
            },
            Availability::ReadOnly => read_only(set_at),
        };
        let result = match self.repo.set_status(&status).await {
            Ok(()) => OpResult::Ack { snapshot: None },
            Err(err) => classify_repository(&err),
        };
        let acked = matches!(result, OpResult::Ack { .. });
        self.log.append(&Record::Outcome { t_ms: now_ms(), id, result })?;
        Ok(acked)
    }

    /// The etag and the bytes of the current version.
    async fn current(&self) -> anyhow::Result<(String, Bytes)> {
        let settings = self.storage.default_settings().await?;
        let ctx = StorageContext::unattributed(&settings);
        match self.storage.get_object_conditional(&ctx, REPO_KEY, None).await? {
            GetModifiedResult::Modified { data, new_version } => {
                let etag = new_version
                    .etag()
                    .cloned()
                    .context("the current version has no etag")?;
                Ok((etag, async_reader_to_bytes(data, 0).await?))
            }
            GetModifiedResult::OnLatestVersion => {
                anyhow::bail!("reading {REPO_KEY} without a version returned no version")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        log::{Sample, read_log},
        oracle::{History, Verdict, mirror::check_readonly_holds},
        repo_mode::tests::{repo_root, repo_worker, who},
        storage_mode::payload_etag,
    };

    async fn toggler_of(root: &RunRoot, config: &RunConfig) -> (Toggler, PathBuf) {
        let who = who("h", TOGGLER_SLOT, 0);
        let log = Arc::new(start_worker_log(root, &who).unwrap());
        let hooks = FaultHooks::sabotage_only(None, Arc::clone(&log));
        let repo = crate::repo_mode::open_repo(root, config, Some(Arc::new(hooks)))
            .await
            .unwrap();
        let storage = storage_mode::storage(root, config, None).await.unwrap();
        let path = root.worker_log(&who.tag, &who.slot, who.incarnation);
        (Toggler::new(repo, storage, root.storage().join(REPO_KEY), log), path)
    }

    async fn availability(root: &RunRoot, config: &RunConfig) -> RepoAvailability {
        let repo = crate::repo_mode::open_repo(root, config, None).await.unwrap();
        repo.get_status().await.unwrap().availability
    }

    fn held(log: &std::path::Path) -> Vec<Record> {
        read_log(log)
            .unwrap()
            .into_iter()
            .filter(|record| matches!(record, Record::ReadOnlyHeld { .. }))
            .collect()
    }

    /// The status changes logged with their outcomes, once resolved.
    fn statuses_set(log: &std::path::Path) -> Vec<(Availability, OpResult)> {
        let records = read_log(log).unwrap();
        records
            .iter()
            .filter_map(|record| match record {
                Record::Intent { id, op: Op::SetStatus { availability }, .. } => {
                    records.iter().find_map(|record| match record {
                        Record::Outcome { id: outcome, result, .. } if outcome == id => {
                            Some((*availability, result.clone()))
                        }
                        _ => None,
                    })
                }
                _ => None,
            })
            .collect()
    }

    /// A hold refuses writers, publishes its version and ends `Online`; the
    /// oracle's M4 passes on its records.
    #[tokio::test]
    async fn a_hold_publishes_its_version_and_refuses_writers() {
        let (_dir, root, config) = repo_root().await;
        let (toggler, log) = toggler_of(&root, &config).await;
        let mut worker = repo_worker(&root, &config, &who("h", "0", 0)).await;
        assert!(worker.ensure_branch().await.unwrap());
        assert!(matches!(
            worker.branch_commit().await.unwrap(),
            Some(OpResult::Ack { .. })
        ));

        let hold =
            tokio::spawn(async move { toggler.hold(Duration::from_millis(500)).await });
        let deadline = Instant::now() + Duration::from_secs(10);
        while availability(&root, &config).await != RepoAvailability::ReadOnly {
            assert!(Instant::now() < deadline, "never ReadOnly");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // A commit can't even start a writable session.
        assert_eq!(worker.branch_commit().await.unwrap(), None);
        let refused = worker.tag().await.unwrap();
        assert!(matches!(refused, Some(OpResult::ReadOnly { .. })), "{refused:?}");
        assert!(hold.await.unwrap().unwrap());

        assert_eq!(availability(&root, &config).await, RepoAvailability::Online);
        let [
            Record::ReadOnlyHeld {
                version: Some(version),
                publish_error: false,
                current,
                mirror,
                t_ack_ms,
                t_end_ms,
            },
        ] = &held(&log)[..]
        else {
            panic!("{:?}", read_log(&log));
        };
        assert_eq!(current, version);
        assert_eq!(*mirror, Sample::Hash(version.clone()));
        assert!(t_end_ms - t_ack_ms >= 500, "{t_ack_ms} {t_end_ms}");
        assert!(matches!(
            &statuses_set(&log)[..],
            [
                (Availability::ReadOnly, OpResult::Ack { .. }),
                (Availability::Online, OpResult::Ack { .. })
            ]
        ));
        let history = History {
            logs: crate::log::read_all_logs(&root).unwrap(),
            ..History::default()
        };
        let mut verdict = Verdict::default();
        check_readonly_holds(&history, false, &mut verdict);
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.stats.readonly_holds, 1);
    }

    /// M4 on real records: a mirror that a publication didn't leave at the
    /// `ReadOnly` version, as when the change skipped its publication.
    #[tokio::test]
    async fn a_mirror_left_behind_during_a_hold_fails_m4() {
        let (_dir, root, config) = repo_root().await;
        let (toggler, log) = toggler_of(&root, &config).await;
        let mirror = root.storage().join(REPO_KEY);
        let before = std::fs::read(&mirror).unwrap();
        let hold =
            tokio::spawn(async move { toggler.hold(Duration::from_millis(500)).await });
        let deadline = Instant::now() + Duration::from_secs(10);
        while !statuses_set(&log).iter().any(|(availability, result)| {
            *availability == Availability::ReadOnly
                && matches!(result, OpResult::Ack { .. })
        }) {
            assert!(Instant::now() < deadline, "never ReadOnly");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        std::fs::write(&mirror, &before).unwrap();
        assert!(hold.await.unwrap().unwrap());

        let history = History {
            logs: crate::log::read_all_logs(&root).unwrap(),
            ..History::default()
        };
        let mut verdict = Verdict::default();
        check_readonly_holds(&history, false, &mut verdict);
        assert!(
            matches!(&verdict.violations[..], [crate::oracle::Violation::ReadOnlyMirrorMismatch {
                detail, ..
            }] if detail.contains(&payload_etag(&before))),
            "{verdict:?}"
        );
    }

    /// Lands a change the way a writer that skips the status check would:
    /// a metadata change through the storage, conditional on the current
    /// version.
    async fn land_a_change_past_the_status(root: &RunRoot, config: &RunConfig) {
        use icechunk::{
            format::snapshot::SnapshotProperties,
            storage::{PutCondition, VersionedUpdateResult},
        };
        let storage = storage_mode::storage(root, config, None).await.unwrap();
        let settings = storage.default_settings().await.unwrap();
        let ctx = StorageContext::unattributed(&settings);
        let GetModifiedResult::Modified { data, new_version } =
            storage.get_object_conditional(&ctx, REPO_KEY, None).await.unwrap()
        else {
            panic!("no current version");
        };
        let info = repo_info_file::decode(&async_reader_to_bytes(data, 0).await.unwrap())
            .unwrap();
        let spec_version = info.spec_version().unwrap();
        let metadata = SnapshotProperties::from([("x".to_owned(), serde_json::json!(1))]);
        let backup = repo_info_file::new_backup_path();
        let changed = info.set_metadata(spec_version, &metadata, &backup, 50).unwrap();
        let bytes = repo_info_file::encode(&changed, spec_version, 3).unwrap();
        let put = storage
            .put_object_conditional(
                &ctx,
                REPO_KEY,
                bytes.into(),
                None,
                Vec::new(),
                PutCondition::IfVersion(&new_version),
            )
            .await
            .unwrap();
        assert!(matches!(put, VersionedUpdateResult::Updated { .. }), "{put:?}");
    }

    /// The version read after the `ReadOnly` change is the toggler's only if
    /// its newest ops-log entry is that change: a change landing in between
    /// is caught.
    #[tokio::test]
    async fn a_change_right_after_the_read_only_change_is_caught() {
        let (_dir, root, config) = repo_root().await;
        let (toggler, _log) = toggler_of(&root, &config).await;
        let set = toggler.set_read_only().await.unwrap().unwrap();
        assert!(!set.publish_error);
        let own = toggler.own_version(&set).await.unwrap();
        let current = toggler.current().await.unwrap().0;
        assert_eq!(own, Some(current));

        land_a_change_past_the_status(&root, &config).await;
        assert_eq!(toggler.own_version(&set).await.unwrap(), None);
    }

    /// A storage error logged during the `ReadOnly` change, as when its
    /// publication can't read the mirror, is recorded, and M4 doesn't judge
    /// that hold's mirror.
    #[tokio::test]
    async fn a_publication_error_during_the_read_only_change_is_recorded() {
        use tracing_subscriber::layer::SubscriberExt as _;
        // The protocol runs on the storage's blocking threads, which only a
        // global subscriber sees.
        let subscriber = tracing_subscriber::registry().with(storage_error_counter());
        tracing::subscriber::set_global_default(subscriber).expect(
            "another global subscriber is set: this test needs a process of its own, as \
             nextest gives it",
        );
        let (_dir, root, config) = repo_root().await;
        let (toggler, log) = toggler_of(&root, &config).await;
        let mirror = root.storage().join(REPO_KEY);
        std::fs::remove_file(&mirror).unwrap();
        #[expect(
            clippy::create_dir,
            reason = "plants a new directory in an existing parent"
        )]
        std::fs::create_dir(&mirror).unwrap();
        std::fs::write(mirror.join("x"), b"x").unwrap();
        assert!(toggler.hold(Duration::from_millis(10)).await.unwrap());
        let [Record::ReadOnlyHeld { publish_error: true, version: Some(_), .. }] =
            &held(&log)[..]
        else {
            panic!("{:?}", read_log(&log));
        };
        let history = History {
            logs: crate::log::read_all_logs(&root).unwrap(),
            ..History::default()
        };
        let mut verdict = Verdict::default();
        check_readonly_holds(&history, false, &mut verdict);
        assert_eq!(verdict.violations, []);
        assert_eq!(verdict.stats.readonly_publish_errors, 1);
    }

    /// Records the targets of the events it sees.
    #[derive(Debug, Clone, Default)]
    struct Recorder(Arc<std::sync::Mutex<Vec<String>>>);

    impl<S: Subscriber> Layer<S> for Recorder {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let target = event.metadata().target().to_owned();
            self.0.lock().unwrap().push(target);
        }
    }

    /// The counter, installed as `fs-stress` installs it, keeps every other
    /// event from no layer.
    #[test]
    fn the_error_counter_leaves_other_events_to_the_other_layers() {
        use tracing_subscriber::layer::SubscriberExt as _;
        let recorder = Recorder::default();
        let subscriber = tracing_subscriber::registry()
            .with(recorder.clone())
            .with(storage_error_counter());
        let before = STORAGE_ERRORS.load(Ordering::Relaxed);
        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(target: "icechunk_fs_stress::sampler", "sampler");
            tracing::error!(target: "icechunk::session", "another crate");
            tracing::warn!(target: "icechunk_fs::repo_d::mirror", "gave up");
            tracing::error!(target: "icechunk_fs_stress::toggler", "not the storage");
            tracing::error!(target: "icechunk_fs::repo_d::commit", "publishing failed");
        });
        assert_eq!(STORAGE_ERRORS.load(Ordering::Relaxed) - before, 1);
        assert_eq!(
            *recorder.0.lock().unwrap(),
            [
                "icechunk_fs_stress::sampler",
                "icechunk::session",
                "icechunk_fs::repo_d::mirror",
                "icechunk_fs_stress::toggler",
                "icechunk_fs::repo_d::commit",
            ]
        );
    }

    #[test]
    fn the_error_counter_counts_only_the_storages_errors() {
        use tracing_subscriber::layer::SubscriberExt as _;
        let subscriber = tracing_subscriber::registry().with(storage_error_counter());
        let before = STORAGE_ERRORS.load(Ordering::Relaxed);
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(target: "icechunk_fs::repo_d::commit", "publishing failed");
            tracing::warn!(target: "icechunk_fs::repo_d::mirror", "gave up");
            tracing::error!(target: "icechunk::session", "another crate");
        });
        assert_eq!(STORAGE_ERRORS.load(Ordering::Relaxed) - before, 1);
    }

    #[tokio::test]
    async fn a_read_only_status_left_behind_is_set_online() {
        let (_dir, root, config) = repo_root().await;
        let repo = crate::repo_mode::open_repo(&root, &config, None).await.unwrap();
        let status = RepoStatus {
            availability: RepoAvailability::ReadOnly,
            set_at: Utc::now(),
            limited_availability_reason: Some(TOGGLER_REASON.to_owned()),
        };
        repo.set_status(&status).await.unwrap();
        let (toggler, log) = toggler_of(&root, &config).await;
        assert!(toggler.ensure_online().await.unwrap());
        assert_eq!(availability(&root, &config).await, RepoAvailability::Online);
        assert!(matches!(
            &statuses_set(&log)[..],
            [(Availability::Online, OpResult::Ack { .. })]
        ));
        // Online already: nothing to do, nothing logged.
        assert!(toggler.ensure_online().await.unwrap());
        assert_eq!(statuses_set(&log).len(), 1);
    }
}
