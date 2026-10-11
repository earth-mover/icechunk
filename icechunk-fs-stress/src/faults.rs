//! Faults injected at the commit protocol's points: kills, pauses and, for
//! the self-test, sabotaged steps.

use std::sync::{Arc, Mutex, PoisonError};

use icechunk_fs::fault_injection::{Point, ProtocolHooks, Sabotage as ProtocolSabotage};
use rand::{RngExt as _, rngs::StdRng};

use crate::{
    config::{FaultConfig, PointName, Sabotage},
    log::{FaultAction, LogWriter, Record, now_ms},
};

#[derive(Debug)]
pub struct FaultHooks {
    config: FaultConfig,
    sabotage: Option<Sabotage>,
    log: Arc<LogWriter>,
    rng: Mutex<StdRng>,
}

impl FaultHooks {
    pub fn new(
        config: &FaultConfig,
        sabotage: Option<Sabotage>,
        log: Arc<LogWriter>,
    ) -> Self {
        Self { config: config.clone(), sabotage, log, rng: Mutex::new(rand::make_rng()) }
    }

    /// A failed append only lowers the fault count, so it never stops the fault.
    fn record(&self, action: FaultAction, point: PointName) {
        let record = Record::Fault { t_ms: now_ms(), action, point };
        if let Err(err) = self.log.append(&record) {
            tracing::warn!("failed to log {record:?}: {err}");
        }
    }
}

impl ProtocolHooks for FaultHooks {
    fn at(&self, point: Point) {
        let point = PointName::from(point);
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
}
