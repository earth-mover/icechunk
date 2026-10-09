//! Faults injected at the conditional-update protocol's points: kills, pauses
//! and, for the self-test, sabotaged steps.

use std::{
    fs, io,
    path::Path,
    sync::{Arc, Mutex, PoisonError},
};

use icechunk_fs::fault_injection::{CasHooks, Point};
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

impl CasHooks for FaultHooks {
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

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        match fs::rename(from, to) {
            Err(err)
                if err.kind() == io::ErrorKind::NotFound
                    && self.sabotage == Some(Sabotage::TrustRenameEnoent) =>
            {
                self.record(FaultAction::Sabotage, PointName::Renamed);
                Ok(())
            }
            result => result,
        }
    }

    fn unlink(&self, path: &Path) -> io::Result<()> {
        if self.sabotage == Some(Sabotage::SkipFence) {
            self.record(FaultAction::Sabotage, PointName::Fenced);
            Ok(())
        } else {
            fs::remove_file(path)
        }
    }

    fn compared(&self, matches: bool) -> bool {
        if self.sabotage == Some(Sabotage::SkipCompare) {
            if !matches {
                self.record(FaultAction::Sabotage, PointName::Compared);
            }
            true
        } else {
            matches
        }
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
    use std::time::Duration;

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
            points: vec![PointName::Compared],
            pause_probability: 1.0,
            ..no_faults()
        };
        let (hooks, path) = hooks(dir.path(), &config, None);
        for point in [
            Point::LockInfoWritten,
            Point::TicketWritten,
            Point::LockTaken { generation: 3 },
            Point::Fenced,
            Point::Renamed,
        ] {
            hooks.at(point);
        }
        assert_eq!(faults(&path), []);

        let started = std::time::Instant::now();
        hooks.at(Point::Compared);
        hooks.at(Point::Compared);
        assert!(started.elapsed() >= 2 * config.pause);
        assert_eq!(faults(&path), [(FaultAction::Pause, PointName::Compared); 2]);
    }

    #[test]
    fn skip_fence_keeps_the_ticket() {
        let dir = tempfile::tempdir().unwrap();
        let ticket = dir.path().join("ticket");
        fs::write(&ticket, b"").unwrap();
        let (hooks, path) = hooks(dir.path(), &no_faults(), Some(Sabotage::SkipFence));
        hooks.unlink(&ticket).unwrap();
        assert!(ticket.exists());
        assert_eq!(faults(&path), [(FaultAction::Sabotage, PointName::Fenced)]);
    }

    #[test]
    fn trust_rename_enoent_reports_success() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        let (hooks, path) =
            hooks(dir.path(), &no_faults(), Some(Sabotage::TrustRenameEnoent));
        hooks.rename(&missing, &dir.path().join("target")).unwrap();
        assert_eq!(faults(&path), [(FaultAction::Sabotage, PointName::Renamed)]);

        let from = dir.path().join("from");
        fs::write(&from, b"x").unwrap();
        hooks.rename(&from, &dir.path().join("target")).unwrap();
        assert_eq!(fs::read(dir.path().join("target")).unwrap(), b"x");
        assert_eq!(faults(&path).len(), 1);
    }

    #[test]
    fn skip_compare_records_only_overridden_verdicts() {
        let dir = tempfile::tempdir().unwrap();
        let (hooks, path) = hooks(dir.path(), &no_faults(), Some(Sabotage::SkipCompare));
        assert!(hooks.compared(true));
        assert_eq!(faults(&path), []);
        assert!(hooks.compared(false));
        assert_eq!(faults(&path), [(FaultAction::Sabotage, PointName::Compared)]);
    }

    #[test]
    fn without_sabotage_the_real_operations_run() {
        let dir = tempfile::tempdir().unwrap();
        let (hooks, path) = hooks(dir.path(), &no_faults(), None);
        let ticket = dir.path().join("ticket");
        fs::write(&ticket, b"").unwrap();
        hooks.unlink(&ticket).unwrap();
        assert!(!ticket.exists());
        let err = hooks.rename(&ticket, &dir.path().join("target")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(!hooks.compared(false));
        assert_eq!(faults(&path), []);
    }
}
