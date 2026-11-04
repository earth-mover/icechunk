//! Faults injected at the commit protocol's points: kills, pauses and, for
//! the self-test, sabotaged steps and one-shot triggers. The hooks also
//! record every mirror rename.

use std::{
    cell::Cell,
    fmt, fs, io,
    path::Path,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use icechunk_fs::fault_injection::{
    Observed, Point, ProtocolHooks, Sabotage as ProtocolSabotage,
};
use rand::{RngExt as _, rngs::StdRng};

use crate::{
    config::{FaultConfig, PointName, Sabotage},
    log::{FaultAction, LogWriter, Record, now_ms},
    storage_mode::{REPO_KEY, payload_etag},
};

thread_local! {
    /// When the publication on this thread last passed its fork check: the
    /// protocol calls `at(BeforeMirrorRename)` and the mirror rename on the
    /// same thread.
    static FORK_CHECK_PASSED_MS: Cell<Option<i64>> = const { Cell::new(None) };
    /// When the fork check of this thread's current publication round last
    /// began reading `$ROOT/repo` (`at(BeforeMirrorRead)`): the read whose
    /// classification let the round go on to its rename. Cleared when a
    /// round stages, so a round that read nothing carries `None`.
    static FORK_CHECK_READ_MS: Cell<Option<i64>> = const { Cell::new(None) };
}

#[derive(Debug)]
pub struct FaultHooks {
    config: FaultConfig,
    sabotage: Option<Sabotage>,
    log: Arc<LogWriter>,
    rng: Mutex<StdRng>,
    trigger: Option<Arc<Trigger>>,
}

/// An action run once, the first time the protocol reaches `point` after the
/// trigger was armed: it makes something happen between two steps of the
/// protocol, deterministically.
pub struct Trigger {
    point: Point,
    armed: AtomicBool,
    action: Box<dyn Fn() + Send + Sync>,
}

impl Trigger {
    /// Disarmed.
    pub fn new(point: Point, action: impl Fn() + Send + Sync + 'static) -> Self {
        Self { point, armed: AtomicBool::new(false), action: Box::new(action) }
    }

    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Whether it was still armed: its action hadn't run.
    pub fn disarm(&self) -> bool {
        self.armed.swap(false, Ordering::SeqCst)
    }

    fn reached(&self, point: Point) {
        if point == self.point && self.disarm() {
            (self.action)();
        }
    }
}

impl fmt::Debug for Trigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Trigger")
            .field("point", &self.point)
            .field("armed", &self.armed.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl FaultHooks {
    pub fn new(
        config: &FaultConfig,
        sabotage: Option<Sabotage>,
        log: Arc<LogWriter>,
    ) -> Self {
        Self {
            config: config.clone(),
            sabotage,
            log,
            rng: Mutex::new(rand::make_rng()),
            trigger: None,
        }
    }

    pub fn with_trigger(mut self, trigger: Arc<Trigger>) -> Self {
        self.trigger = Some(trigger);
        self
    }

    /// No kills or pauses, only `sabotage` and the records.
    pub fn sabotage_only(sabotage: Option<Sabotage>, log: Arc<LogWriter>) -> Self {
        let config = FaultConfig {
            points: vec![],
            kill_probability: 0.0,
            pause_probability: 0.0,
            pause: Duration::ZERO,
        };
        Self::new(&config, sabotage, log)
    }

    /// A failed append only lowers the fault count, so it never stops the fault.
    fn record(&self, action: FaultAction, point: PointName) {
        self.append(&Record::Fault { t_ms: now_ms(), action, point });
    }

    fn append(&self, record: &Record) {
        if let Err(err) = self.log.append(record) {
            tracing::warn!("failed to log {record:?}: {err}");
        }
    }
}

/// The name of an [`Observed`] in logs and reports.
pub fn observed_name(what: Observed) -> &'static str {
    match what {
        Observed::LinkExisted => "link_existed",
        Observed::RenameLanded => "rename_enoent.landed",
        Observed::RenameLostRace => "rename_enoent.lost_race",
        Observed::RenameSuperseded => "rename_enoent.superseded",
        Observed::RenameUnproven => "rename_enoent.unproven",
    }
}

/// Whether `from` and `to` are a publication's rename of its stage
/// `repo.d/mirror.<id>` over the mirror `repo` next to `repo.d`.
fn is_mirror_rename(from: &Path, to: &Path) -> bool {
    fn name(path: &Path) -> Option<&str> {
        path.file_name().and_then(|name| name.to_str())
    }
    let stage_dir = from.parent();
    name(to) == Some(REPO_KEY)
        && name(from).is_some_and(|name| name.starts_with("mirror."))
        && stage_dir.and_then(name) == Some("repo.d")
        && stage_dir.and_then(Path::parent) == to.parent()
}

impl ProtocolHooks for FaultHooks {
    fn at(&self, point: Point) {
        match point {
            Point::MirrorStaged => FORK_CHECK_READ_MS.set(None),
            Point::BeforeMirrorRead => FORK_CHECK_READ_MS.set(Some(now_ms())),
            Point::BeforeMirrorRename => FORK_CHECK_PASSED_MS.set(Some(now_ms())),
            _ => {}
        }
        if let Some(trigger) = &self.trigger {
            trigger.reached(point);
        }
        let Ok(point) = PointName::try_from(point) else { return };
        if !self.config.points.contains(&point) {
            return;
        }
        let u: f64 = self.rng.lock().unwrap_or_else(PoisonError::into_inner).random();
        if u < self.config.kill_probability {
            self.record(FaultAction::Kill, point);
            kill_self();
        } else if u < self.config.kill_probability + self.config.pause_probability {
            self.record(FaultAction::Pause, point);
            // Hooks run on the storage's blocking threads.
            std::thread::sleep(self.config.pause);
        }
    }

    /// Records every `true`: each one takes the sabotaged branch, though it
    /// may find nothing to break, as a tidy-up with no file above the marker.
    fn sabotaged(&self, sabotage: ProtocolSabotage) -> bool {
        let sabotage = Sabotage::from(sabotage);
        let on = self.sabotage == Some(sabotage);
        if on {
            self.record(FaultAction::Sabotage, sabotage.point());
        }
        on
    }

    /// Records a mirror rename before it, and again once it succeeded, with
    /// the hash of the bytes it installs: a process killed in between leaves
    /// only the first record. A stage that can't be read is renamed
    /// unrecorded: the rename then fails too, unless the stage appeared in
    /// between.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        if !is_mirror_rename(from, to) {
            return fs::rename(from, to);
        }
        let t_check_ms = FORK_CHECK_PASSED_MS.take();
        let t_read_ms = FORK_CHECK_READ_MS.take();
        let hash = fs::read(from).map(|bytes| payload_etag(&bytes));
        if let Ok(hash) = &hash {
            let hash = hash.clone();
            self.append(&Record::MirrorRenaming {
                t_check_ms,
                t_read_ms,
                t_ms: now_ms(),
                hash,
            });
        }
        fs::rename(from, to)?;
        if let Ok(hash) = hash {
            self.append(&Record::MirrorRename {
                t_check_ms,
                t_read_ms,
                t_rename_ms: now_ms(),
                hash,
            });
        }
        Ok(())
    }

    fn observed(&self, what: Observed) {
        self.append(&Record::Observed {
            t_ms: now_ms(),
            what: observed_name(what).to_owned(),
        });
    }
}

/// Dies by `SIGKILL`, as a crash would: no destructors, no buffered output.
pub fn kill_self() -> ! {
    // SAFETY: `kill` and `getpid` take and return plain integers.
    #[expect(unsafe_code)]
    unsafe {
        libc::kill(libc::getpid(), libc::SIGKILL);
    }
    // The signal may be delivered after `kill` returns.
    loop {
        std::thread::park();
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, time::Duration};

    use super::*;
    use crate::log::read_log;

    fn hooks(
        dir: &Path,
        config: &FaultConfig,
        sabotage: Option<Sabotage>,
    ) -> (FaultHooks, std::path::PathBuf) {
        let path = dir.join("h.0.0.log");
        let log = Arc::new(LogWriter::create(&path).unwrap());
        (FaultHooks::new(config, sabotage, log), path)
    }

    fn no_faults() -> FaultConfig {
        FaultConfig {
            points: vec![],
            kill_probability: 0.0,
            pause_probability: 0.0,
            pause: Duration::from_millis(10),
        }
    }

    fn faults_of(path: &Path) -> Vec<(FaultAction, PointName)> {
        read_log(path)
            .unwrap()
            .into_iter()
            .filter_map(|record| match record {
                Record::Fault { action, point, .. } => Some((action, point)),
                _ => None,
            })
            .collect()
    }

    fn faults(path: &Path) -> Vec<(FaultAction, PointName)> {
        read_log(path)
            .unwrap()
            .into_iter()
            .map(|record| match record {
                Record::Fault { action, point, .. } => (action, point),
                other => panic!("unexpected record {other:?}"),
            })
            .collect()
    }

    #[test]
    fn pauses_only_at_configured_points() {
        let dir = tempfile::tempdir().unwrap();
        let config = FaultConfig {
            points: vec![PointName::BeforeRename],
            pause_probability: 1.0,
            ..no_faults()
        };
        let (hooks, path) = hooks(dir.path(), &config, None);
        for point in [Point::Staged, Point::Linked, Point::Renamed, Point::BeforeTidyUp] {
            hooks.at(point);
        }
        assert_eq!(faults(&path), []);

        let started = std::time::Instant::now();
        hooks.at(Point::BeforeRename);
        hooks.at(Point::BeforeRename);
        assert!(started.elapsed() >= 2 * config.pause);
        assert_eq!(faults(&path), [(FaultAction::Pause, PointName::BeforeRename); 2]);
    }

    #[test]
    fn only_the_configured_sabotage_is_on_and_each_use_is_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let (hooks, path) =
            hooks(dir.path(), &no_faults(), Some(Sabotage::DeleteWinnerFile));
        assert!(!hooks.sabotaged(ProtocolSabotage::SkipLink));
        assert!(!hooks.sabotaged(ProtocolSabotage::TidyUpDeletesCurrent));
        assert_eq!(faults(&path), []);
        assert!(hooks.sabotaged(ProtocolSabotage::DeleteWinnerFile));
        assert!(hooks.sabotaged(ProtocolSabotage::DeleteWinnerFile));
        assert_eq!(faults(&path), [(FaultAction::Sabotage, PointName::Renamed); 2]);
    }

    #[test]
    fn without_sabotage_nothing_is_sabotaged() {
        let dir = tempfile::tempdir().unwrap();
        let (hooks, path) = hooks(dir.path(), &no_faults(), None);
        assert!(!hooks.sabotaged(ProtocolSabotage::SkipLink));
        assert!(!hooks.sabotaged(ProtocolSabotage::AckUnproven));
        assert_eq!(faults(&path), []);
    }

    #[test]
    fn mirror_renames_are_recorded_with_the_hash_and_the_fork_check_time() {
        let dir = tempfile::tempdir().unwrap();
        let (hooks, path) = hooks(dir.path(), &no_faults(), None);
        let root = dir.path().join("storage");
        let repo_d = root.join("repo.d");
        fs::create_dir_all(&repo_d).unwrap();
        let stage = repo_d.join("mirror.1");
        let mirror = root.join(REPO_KEY);
        let records = || {
            read_log(&path)
                .unwrap()
                .into_iter()
                .filter_map(|record| match record {
                    Record::MirrorRename { t_check_ms, t_read_ms, t_rename_ms, hash } => {
                        Some((t_check_ms, t_read_ms, t_rename_ms, hash))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };

        // Not a mirror rename: a content file, or a stage elsewhere.
        fs::write(repo_d.join("stage.1"), b"v").unwrap();
        hooks.rename(&repo_d.join("stage.1"), &repo_d.join("1.h")).unwrap();
        fs::write(root.join("mirror.2"), b"v").unwrap();
        hooks.rename(&root.join("mirror.2"), &mirror).unwrap();
        assert_eq!(records(), []);

        // A round whose check read twice, the last read before `reread`.
        fs::write(&stage, b"v1").unwrap();
        hooks.at(Point::MirrorStaged);
        hooks.at(Point::BeforeMirrorRead);
        std::thread::sleep(Duration::from_millis(2));
        let reread = now_ms();
        hooks.at(Point::BeforeMirrorRead);
        hooks.at(Point::BeforeMirrorRename);
        let checked = now_ms();
        hooks.rename(&stage, &mirror).unwrap();
        // A round that read nothing, after a read of an earlier round.
        fs::write(&stage, b"v2").unwrap();
        hooks.at(Point::BeforeMirrorRead);
        hooks.at(Point::MirrorStaged);
        hooks.rename(&stage, &mirror).unwrap();
        // A stage that is gone: the rename fails, unrecorded.
        assert!(hooks.rename(&stage, &mirror).is_err());

        let recorded = records();
        let [(Some(t_check), Some(t_read), t_rename, first), (None, None, _, second)] =
            &recorded[..]
        else {
            panic!("{recorded:?}");
        };
        assert!(reread <= *t_read && *t_read <= *t_check, "{recorded:?}");
        assert!(*t_check <= checked && checked <= *t_rename, "{recorded:?}");
        assert_eq!(*first, payload_etag(b"v1"));
        assert_eq!(*second, payload_etag(b"v2"));
        assert_eq!(fs::read(&mirror).unwrap(), b"v2");
        // Recorded whatever the configured fault points.
        assert_eq!(faults_of(&path), []);
        // Each rename of a readable stage is announced first.
        let announced: Vec<String> = read_log(&path)
            .unwrap()
            .into_iter()
            .filter_map(|record| match record {
                Record::MirrorRenaming { hash, .. } => Some(hash),
                _ => None,
            })
            .collect();
        assert_eq!(announced, [payload_etag(b"v1"), payload_etag(b"v2")]);
    }

    #[test]
    fn a_trigger_runs_once_per_arming_at_its_point() {
        use std::sync::atomic::AtomicUsize;
        let dir = tempfile::tempdir().unwrap();
        let fired = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&fired);
        let trigger = Arc::new(Trigger::new(Point::MirrorRenamed, move || {
            counter.fetch_add(1, Ordering::SeqCst);
        }));
        let (hooks, path) = hooks(dir.path(), &no_faults(), None);
        let hooks = hooks.with_trigger(Arc::clone(&trigger));
        hooks.at(Point::MirrorRenamed);
        assert_eq!(fired.load(Ordering::SeqCst), 0, "disarmed");
        trigger.arm();
        hooks.at(Point::BeforeMirrorRename);
        assert_eq!(fired.load(Ordering::SeqCst), 0, "another point");
        hooks.at(Point::MirrorRenamed);
        hooks.at(Point::MirrorRenamed);
        assert_eq!(fired.load(Ordering::SeqCst), 1);
        assert!(!trigger.disarm(), "it ran");
        trigger.arm();
        assert!(trigger.disarm(), "it didn't run");
        hooks.at(Point::MirrorRenamed);
        assert_eq!(fired.load(Ordering::SeqCst), 1);
        assert_eq!(faults(&path), []);
    }

    #[test]
    fn conversion_steps_are_fault_points() {
        use icechunk_fs::fault_injection::{ConversionStep, ToObjectStoreStep};
        let dir = tempfile::tempdir().unwrap();
        let config = FaultConfig {
            points: vec![PointName::ToObjectStoreChange],
            pause_probability: 1.0,
            ..no_faults()
        };
        let (hooks, path) = hooks(dir.path(), &config, None);
        hooks.at(Point::Conversion(ConversionStep::ToObjectStore(
            ToObjectStoreStep::ForkCheck,
        )));
        hooks.at(Point::Conversion(ConversionStep::ToObjectStore(
            ToObjectStoreStep::Change,
        )));
        assert_eq!(faults(&path), [(FaultAction::Pause, PointName::ToObjectStoreChange)]);
    }

    #[test]
    fn sabotage_only_hooks_neither_kill_nor_pause() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.0.0.log");
        let log = Arc::new(LogWriter::create(&path).unwrap());
        let hooks = FaultHooks::sabotage_only(Some(Sabotage::DeleteWinnerFile), log);
        for point in [Point::Linked, Point::BeforeRename, Point::BeforeMirrorRename] {
            hooks.at(point);
        }
        assert!(hooks.sabotaged(ProtocolSabotage::DeleteWinnerFile));
        assert!(!hooks.sabotaged(ProtocolSabotage::SkipLink));
        assert_eq!(faults_of(&path), [(FaultAction::Sabotage, PointName::Renamed)]);
    }
}
