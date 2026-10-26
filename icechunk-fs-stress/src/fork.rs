//! The fork actor: a worker slot on the first participant that, `fork.at`
//! after the go, replaces `$ROOT/repo` with a repo info without the writer
//! feature, as a status change by Icechunk 2.0 to 2.3 through the mirror
//! does, then watches for the fork to be erased. Plain `std::fs`, as 2.3's
//! local backend writes: `new_local_filesystem_storage` would refuse it.

use std::{
    fs, io,
    path::Path,
    time::{Duration, Instant, SystemTime},
};

use anyhow::Context as _;
use chrono::{DateTime, TimeDelta, Utc};
use icechunk::format::{
    OVERWRITTEN_FILES_PATH,
    envelope::DEFAULT_COMPRESSION_LEVEL,
    layout_v1,
    repo_info::{
        DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE, RepoAvailability, RepoInfo, RepoStatus,
    },
    repo_info_file,
    writer_features::WriterFeature,
};
use uuid::Uuid;

use crate::{
    config::{RunConfig, WorkerId},
    control::StopCheck,
    layout::RunRoot,
    log::{LogWriter, Record, Sample, now_ms, read_log, start_worker_log},
    sampler::sample,
    storage_mode::{REPO_KEY, payload_etag},
};

/// The slot of the fork actor.
pub const FORK_SLOT: &str = "fork";

/// How often the actor reads `$ROOT/repo` while its fork is in place.
const POLL: Duration = Duration::from_millis(10);
/// With `fork.repeat`, how long after an erasure the actor forks again.
const REFORK_AFTER: Duration = Duration::from_millis(500);

/// A fork [`write_fork`] wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fork {
    pub t_before_ms: i64,
    pub t_after_ms: i64,
    pub base_hash: String,
    pub fork_hash: String,
    pub backup: String,
}

impl Fork {
    /// Its [`Record::ForkWritten`]; `gc` is the GC it was written during.
    pub fn record(&self, gc: Option<Uuid>) -> Record {
        Record::ForkWritten {
            t_before_ms: self.t_before_ms,
            t_after_ms: self.t_after_ms,
            base_hash: self.base_hash.clone(),
            fork_hash: self.fork_hash.clone(),
            backup: self.backup.clone(),
            gc,
        }
    }
}

/// The repo info Icechunk 2.0 to 2.3 write when they set the status `Online`
/// on `base`, naming `backup` on its newest entry: without writer features
/// and without the gate. A `base` that lists the feature loses it, through
/// the format's only way to change writer features, and then its status is
/// set silently; a standard `base` gets the status change.
pub fn fork_bytes(base: &[u8], backup: &str) -> anyhow::Result<Vec<u8>> {
    let format = |what: &'static str| {
        move |err: icechunk::format::IcechunkFormatError| {
            anyhow::anyhow!("{what}: {}", err.kind())
        }
    };
    let info = repo_info_file::decode(base).map_err(format("decoding the base"))?;
    let spec = info.spec_version().map_err(format("reading the spec version"))?;
    let num_updates = info
        .num_updates_per_repo_info_file()
        .map_err(format("reading the configuration"))?
        .unwrap_or(DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE);
    let now = update_time(&info).map_err(format("reading the ops log"))?;
    let online = RepoStatus {
        availability: RepoAvailability::Online,
        set_at: now,
        limited_availability_reason: None,
    };
    let lists_feature =
        layout_v1::lists_feature(base).map_err(format("decoding the base"))?;
    let fork = if lists_feature {
        info.with_writer_features(
            spec,
            &[],
            &[WriterFeature::FilesystemLayoutV1],
            backup,
            now,
            num_updates,
        )
        .and_then(|info| info.set_status(spec, &online, None))
    } else {
        info.set_status(spec, &online, Some((backup, num_updates)))
    }
    .map_err(format("building the fork"))?;
    repo_info_file::encode(&fork, spec, DEFAULT_COMPRESSION_LEVEL)
        .map_err(format("encoding the fork"))
}

/// Now, to the microsecond, above the newest ops-log entry's time.
fn update_time(info: &RepoInfo) -> icechunk::format::IcechunkResult<DateTime<Utc>> {
    let now = Utc::now();
    let now = DateTime::from_timestamp_micros(now.timestamp_micros()).unwrap_or(now);
    Ok(match info.latest_updates()?.next().transpose()? {
        Some((_, newest, _)) if newest >= now => newest + TimeDelta::microseconds(1),
        _ => now,
    })
}

/// Logs [`Record::ForkBackup`] for a new backup path and returns it, for
/// [`write_fork`]: an interrupted copy leaves a partial backup that no ops
/// log names, which the oracle then knows.
pub fn announce_backup(log: &LogWriter) -> io::Result<String> {
    let backup = repo_info_file::new_backup_path();
    log.append(&Record::ForkBackup { t_ms: now_ms(), backup: backup.clone() })?;
    Ok(backup)
}

/// Forks the repository in `storage_root` as Icechunk 2.3's local backend
/// writes a status change: copies `$ROOT/repo` to `overwritten/<backup>`, a
/// path from [`announce_backup`], writes the fork, derived from that copy, to
/// a temporary file, and renames it over `$ROOT/repo`.
pub fn write_fork(storage_root: &Path, backup: String) -> anyhow::Result<Fork> {
    let t_before_ms = now_ms();
    let mirror = storage_root.join(REPO_KEY);
    let backup_path = storage_root.join(OVERWRITTEN_FILES_PATH).join(&backup);
    let dir = storage_root.join(OVERWRITTEN_FILES_PATH);
    fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    fs::copy(&mirror, &backup_path).with_context(|| {
        format!("copying {} to {}", mirror.display(), backup_path.display())
    })?;
    let base = fs::read(&backup_path)
        .with_context(|| format!("reading {}", backup_path.display()))?;
    let bytes = fork_bytes(&base, &backup)?;
    let temporary = storage_root.join(format!("{REPO_KEY}#{}", rand::random::<u32>()));
    fs::write(&temporary, &bytes)
        .and_then(|()| fs::rename(&temporary, &mirror))
        .with_context(|| format!("replacing {}", mirror.display()))?;
    Ok(Fork {
        t_before_ms,
        t_after_ms: now_ms(),
        base_hash: payload_etag(&base),
        fork_hash: payload_etag(&bytes),
        backup,
    })
}

/// What replaced the fork `fork_hash` in `$ROOT/repo`; `None` while it is
/// there, or can't be read.
pub fn erased(storage_root: &Path, fork_hash: &str) -> Option<Sample> {
    match sample(&storage_root.join(REPO_KEY)) {
        Sample::Hash(hash) if hash == fork_hash => None,
        Sample::Error(_) => None,
        found => Some(found),
    }
}

/// What the actor does next.
#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    ForkAt(Instant),
    Watching(String),
    Done,
}

/// Runs the fork actor until `until` or a `stop` control: forks `fork.at`
/// after the go, announcing its backup, and logs [`Record::ForkWritten`],
/// then polls `$ROOT/repo`
/// and logs [`Record::ForkGone`] once the fork is erased; with `fork.repeat`
/// it forks again [`REFORK_AFTER`] later. A new incarnation goes on from
/// what its predecessors logged. Only failing to write its log is fatal.
pub async fn fork_actor(
    root: &RunRoot,
    config: &RunConfig,
    who: &WorkerId,
    until: SystemTime,
) -> anyhow::Result<()> {
    let fork = config.fork.context("the run has no fork configuration")?;
    let log = start_worker_log(root, who)?;
    let storage = root.storage();
    let go = until.checked_sub(config.duration).unwrap_or(until);
    let fork_at = Instant::now()
        + (go + fork.at).duration_since(SystemTime::now()).unwrap_or_default();
    let mut state = match predecessors(root, who)? {
        Earlier::Nothing => State::ForkAt(fork_at),
        Earlier::Watching(hash) => State::Watching(hash),
        Earlier::Erased if fork.repeat => State::ForkAt(Instant::now() + REFORK_AFTER),
        Earlier::Erased => State::Done,
    };
    let mut stop = StopCheck::new(root, until);
    while !stop.stopped() {
        state = match state {
            State::ForkAt(at) if Instant::now() < at => {
                tokio::time::sleep(POLL.min(at - Instant::now())).await;
                State::ForkAt(at)
            }
            State::ForkAt(_) => match write_fork(&storage, announce_backup(&log)?) {
                Ok(written) => {
                    log.append(&written.record(None))?;
                    State::Watching(written.fork_hash)
                }
                Err(err) => {
                    tracing::warn!("writing a fork: {err:#}");
                    State::ForkAt(Instant::now() + REFORK_AFTER)
                }
            },
            State::Watching(fork_hash) => match erased(&storage, &fork_hash) {
                None => {
                    tokio::time::sleep(POLL).await;
                    State::Watching(fork_hash)
                }
                Some(found) => {
                    log.append(&Record::ForkGone { t_ms: now_ms(), fork_hash, found })?;
                    if fork.repeat {
                        State::ForkAt(Instant::now() + REFORK_AFTER)
                    } else {
                        State::Done
                    }
                }
            },
            State::Done => {
                tokio::time::sleep(POLL * 10).await;
                State::Done
            }
        };
    }
    Ok(())
}

/// What the earlier incarnations of the fork slot left.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Earlier {
    Nothing,
    /// A fork they wrote and didn't see erased.
    Watching(String),
    Erased,
}

fn predecessors(root: &RunRoot, who: &WorkerId) -> anyhow::Result<Earlier> {
    let mut earlier = Earlier::Nothing;
    for incarnation in 0..who.incarnation {
        let path = root.worker_log(&who.tag, &who.slot, incarnation);
        let records = match read_log(&path) {
            Ok(records) => records,
            Err(err) if is_missing(&err) => continue,
            Err(err) => return Err(err),
        };
        for record in records {
            match record {
                Record::ForkWritten { fork_hash, .. } => {
                    earlier = Earlier::Watching(fork_hash);
                }
                Record::ForkGone { .. } => earlier = Earlier::Erased,
                _ => {}
            }
        }
    }
    Ok(earlier)
}

fn is_missing(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<io::Error>()
            .is_some_and(|err| err.kind() == io::ErrorKind::NotFound)
    })
}

/// Appends `fork`'s record to `log`, warning if it can't.
pub fn log_fork(log: &LogWriter, fork: &Fork, gc: Option<Uuid>) {
    if let Err(err) = log.append(&fork.record(gc)) {
        tracing::warn!("logging a fork: {err}");
    }
}

#[cfg(test)]
mod tests {
    use icechunk::format::repo_info::UpdateType;

    use super::*;
    use crate::{
        config::Mode,
        storage_mode::{self, read_repo},
        supervisor,
    };

    #[test]
    fn a_fork_drops_the_feature_and_the_gate_in_one_status_change() {
        let base = storage_mode::initial_repo_info(&[WriterFeature::FilesystemLayoutV1])
            .unwrap();
        let fork = fork_bytes(&base, "repo.1.X").unwrap();
        assert!(!layout_v1::lists_feature(&fork).unwrap());
        let info = repo_info_file::decode(&fork).unwrap();
        assert!(info.raw_writer_features().unwrap().is_empty());
        let header = info.header().unwrap();
        assert!(header.writer_features().is_empty());
        assert_eq!(info.status().unwrap().availability, RepoAvailability::Online);
        let updates: Vec<_> =
            info.latest_updates().unwrap().map(Result::unwrap).collect();
        assert_eq!(updates.len(), 2, "{updates:?}");
        assert!(matches!(updates[0].0, UpdateType::RepoStatusChangedUpdate { .. }));
        assert_eq!(
            layout_v1::change_info(&fork)
                .unwrap()
                .attempt
                .map(|attempt| attempt.id)
                .as_deref(),
            Some("repo.1.X")
        );

        // A standard repo info gets a status change.
        let standard = storage_mode::initial_repo_info(&[]).unwrap();
        let fork = fork_bytes(&standard, "repo.2.Y").unwrap();
        assert!(!layout_v1::lists_feature(&fork).unwrap());
        assert_eq!(
            layout_v1::change_info(&fork)
                .unwrap()
                .attempt
                .map(|attempt| attempt.id)
                .as_deref(),
            Some("repo.2.Y")
        );
        assert_ne!(payload_etag(&fork), payload_etag(&standard));
    }

    #[tokio::test]
    async fn a_fork_is_written_as_23_writes_it_and_its_erasure_is_seen() {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().join("run"));
        let config = RunConfig::defaults(Mode::Storage, 1);
        supervisor::init(&root, &config).await.unwrap();
        let storage = root.storage();
        let mirror = fs::read(storage.join(REPO_KEY)).unwrap();

        let who =
            WorkerId { tag: "h".to_owned(), slot: FORK_SLOT.to_owned(), incarnation: 0 };
        let log = start_worker_log(&root, &who).unwrap();
        let fork = write_fork(&storage, announce_backup(&log).unwrap()).unwrap();
        let records = read_log(&root.worker_log("h", FORK_SLOT, 0)).unwrap();
        assert!(
            matches!(&records[..], [Record::Start { .. }, Record::ForkBackup { backup, .. }]
                if *backup == fork.backup),
            "{records:?}"
        );
        assert!(fork.t_before_ms <= fork.t_after_ms);
        assert_eq!(fork.base_hash, payload_etag(&mirror));
        let backup =
            fs::read(storage.join(OVERWRITTEN_FILES_PATH).join(&fork.backup)).unwrap();
        assert_eq!(backup, mirror);
        let forked = fs::read(storage.join(REPO_KEY)).unwrap();
        assert_eq!(fork.fork_hash, payload_etag(&forked));
        assert!(!layout_v1::lists_feature(&forked).unwrap());
        let leftovers: Vec<_> = fs::read_dir(&storage)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name.starts_with("repo#"))
            .collect();
        assert_eq!(leftovers, Vec::<String>::new());
        // `repo.d` is untouched: the storage still reads its current version.
        let s = storage_mode::storage(&root, &config, None).await.unwrap();
        assert_eq!(read_repo(&*s).await.unwrap(), mirror);

        assert_eq!(erased(&storage, &fork.fork_hash), None);
        fs::write(storage.join(REPO_KEY), &mirror).unwrap();
        assert_eq!(
            erased(&storage, &fork.fork_hash),
            Some(Sample::Hash(payload_etag(&mirror)))
        );
        fs::remove_file(storage.join(REPO_KEY)).unwrap();
        assert_eq!(erased(&storage, &fork.fork_hash), Some(Sample::Missing));
    }

    #[tokio::test]
    async fn the_actor_forks_watches_and_forks_again() {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().join("run"));
        let mut config = RunConfig::defaults(Mode::Storage, 1);
        config.duration = Duration::from_secs(3);
        config.fork = Some(crate::config::ForkConfig {
            at: Duration::from_millis(100),
            repeat: true,
            during_gc: false,
        });
        supervisor::init(&root, &config).await.unwrap();
        let storage = root.storage();
        let mirror = fs::read(storage.join(REPO_KEY)).unwrap();
        let who =
            WorkerId { tag: "h".to_owned(), slot: FORK_SLOT.to_owned(), incarnation: 0 };
        let until = SystemTime::now() + config.duration;
        let actor = {
            let (root, config, who) = (root.clone(), config.clone(), who.clone());
            tokio::spawn(async move { fork_actor(&root, &config, &who, until).await })
        };
        let forked =
            || fs::read(storage.join(REPO_KEY)).is_ok_and(|bytes| bytes != mirror);
        while !forked() {
            tokio::time::sleep(POLL).await;
        }
        // An erasure, as a publication without the fork check makes one.
        fs::write(storage.join(REPO_KEY), &mirror).unwrap();
        actor.await.unwrap().unwrap();
        let records = read_log(&root.worker_log("h", FORK_SLOT, 0)).unwrap();
        let kinds: Vec<_> = records
            .iter()
            .filter_map(|record| match record {
                Record::ForkBackup { .. } => Some("backup"),
                Record::ForkWritten { gc: None, .. } => Some("written"),
                Record::ForkGone { found: Sample::Hash(hash), .. }
                    if *hash == payload_etag(&mirror) =>
                {
                    Some("gone")
                }
                Record::Start { .. } => None,
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(
            kinds,
            ["backup", "written", "gone", "backup", "written"],
            "{records:?}"
        );

        // A new incarnation keeps watching the fork its predecessor wrote.
        let next = WorkerId { incarnation: 1, ..who };
        assert!(matches!(predecessors(&root, &next).unwrap(), Earlier::Watching(_)));
    }
}
