//! `report.json`, written by `verify`, and its human summary.

use std::{collections::BTreeSet, fmt::Write as _, fs, time::Duration};

use anyhow::Context as _;
use chrono::DateTime;
use serde::{Deserialize, Serialize};

use crate::{
    config::{Backend, Mode, RunConfig},
    env_info::{HostDone, HostInfo},
    layout::RunRoot,
    oracle::{Verdict, Violation, Warning, liveness::EXTERNAL_KILL_FLOOR},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub passed: bool,
    pub verdict: Verdict,
    pub config: RunConfig,
    pub participants: Participants,
    pub hosts: Vec<HostInfo>,
    pub done: Vec<HostDone>,
    pub recovery_ms: Option<u64>,
    pub notes: Vec<String>,
}

/// The hosts decided at the go.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Participants {
    pub tags: BTreeSet<String>,
    /// Those without a `done/` entry when verify gave up waiting: they may
    /// still have been writing, so the verdict is provisional.
    pub missing: Vec<String>,
}

/// Violations and warnings shown in full in the summary.
const SHOWN: usize = 20;
const SHOWN_NAMES: usize = 10;

impl Report {
    /// A report that passed if `verdict` has no violations.
    pub fn new(
        verdict: Verdict,
        config: RunConfig,
        participants: Participants,
        hosts: Vec<HostInfo>,
        done: Vec<HostDone>,
        recovery_ms: Option<u64>,
        notes: Vec<String>,
    ) -> Self {
        Self {
            passed: verdict.violations.is_empty(),
            verdict,
            config,
            participants,
            hosts,
            done,
            recovery_ms,
            notes,
        }
    }

    /// Writes `report.json`, replacing any earlier one by a rename.
    pub fn write(&self, root: &RunRoot) -> anyhow::Result<()> {
        let path = root.report_json();
        let staging =
            root.root().join(format!("report.json.{}.tmp", uuid::Uuid::new_v4()));
        let written = fs::write(&staging, serde_json::to_vec_pretty(self)?)
            .and_then(|()| fs::rename(&staging, &path));
        if written.is_err() {
            // Best effort: the error being reported is the write's.
            let _ = fs::remove_file(&staging);
        }
        written.with_context(|| format!("writing {}", path.display()))
    }

    pub fn load(root: &RunRoot) -> anyhow::Result<Self> {
        let path = root.report_json();
        let bytes =
            fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))
    }

    /// 0 if the run passed, 3 if only the harness failed or the verdict is
    /// provisional, 1 otherwise.
    pub fn exit_code(&self) -> u8 {
        let violations = &self.verdict.violations;
        if violations.is_empty() {
            0
        } else if self.is_provisional()
            || violations.iter().all(|v| matches!(v, Violation::Harness { .. }))
        {
            3
        } else {
            1
        }
    }

    fn is_provisional(&self) -> bool {
        !self.participants.missing.is_empty()
    }

    pub fn summary(&self) -> String {
        let mut out = String::new();
        // Writing to a `String` can't fail.
        let _ = self.write_summary(&mut out);
        out
    }

    fn write_summary(&self, out: &mut String) -> std::fmt::Result {
        let config = &self.config;
        let mode = match config.mode {
            Mode::Storage => "storage",
            Mode::Repository => "repository",
        };
        let backend = match config.backend {
            Backend::Filesystem => String::new(),
            backend => format!(" backend: {} (unsafe),", backend.name()),
        };
        writeln!(
            out,
            "{}: {mode} mode,{backend} {} host(s), {}",
            if self.passed { "PASS" } else { "FAIL" },
            self.participants.tags.len(),
            duration(config.duration),
        )?;
        if self.is_provisional() {
            writeln!(
                out,
                "provisional: hosts still missing ({}); verify again once they finished",
                self.participants.missing.join(", ")
            )?;
        }
        for host in &self.hosts {
            writeln!(
                out,
                "  {}: {}, {} ({}), kernel {}",
                host.tag,
                host.hostname,
                host.fs_type,
                if host.network_fs { "network" } else { "local" },
                host.kernel
            )?;
        }
        let stats = &self.verdict.stats;
        writeln!(
            out,
            "stats: {} intents, {} acks, {} conflicts, {} errors, {} unresolved; \
             chain length {}; {} external kills",
            stats.intents,
            stats.acks,
            stats.conflicts,
            stats.errors,
            stats.unresolved,
            stats.chain_len,
            stats.external_kills
        )?;
        if !stats.faults.is_empty() {
            let faults: Vec<_> =
                stats.faults.iter().map(|(kind, n)| format!("{kind} {n}")).collect();
            writeln!(out, "faults: {}", faults.join(", "))?;
        }
        let violations: Vec<_> = self.verdict.violations.iter().map(violation).collect();
        list(out, "violations", &violations)?;
        let warnings: Vec<_> = self.verdict.warnings.iter().map(warning).collect();
        list(out, "warnings", &warnings)?;
        match self.recovery_ms {
            Some(ms) => writeln!(out, "recovery write: {ms} ms")?,
            None => writeln!(out, "recovery write: none")?,
        }
        if !self.notes.is_empty() {
            writeln!(out, "notes:")?;
            for note in &self.notes {
                writeln!(out, "  {note}")?;
            }
        }
        Ok(())
    }
}

fn list(out: &mut String, what: &str, items: &[String]) -> std::fmt::Result {
    if items.is_empty() {
        return Ok(());
    }
    writeln!(out, "{what} ({}):", items.len())?;
    for item in items.iter().take(SHOWN) {
        writeln!(out, "  {item}")?;
    }
    if items.len() > SHOWN {
        writeln!(out, "  … and {} more (see report.json)", items.len() - SHOWN)?;
    }
    Ok(())
}

fn duration(d: Duration) -> String {
    if d.subsec_millis() == 0 {
        format!("{}s", d.as_secs())
    } else {
        format!("{}ms", d.as_millis())
    }
}

fn time(t_ms: i64) -> String {
    DateTime::from_timestamp_millis(t_ms)
        .map_or_else(|| format!("{t_ms} ms"), |t| t.format("%H:%M:%S%.3f").to_string())
}

fn names(names: &[String]) -> String {
    let shown = names.iter().take(SHOWN_NAMES).cloned().collect::<Vec<_>>().join(", ");
    if names.len() > SHOWN_NAMES {
        format!("{shown} and {} more", names.len() - SHOWN_NAMES)
    } else {
        shown
    }
}

fn violation(v: &Violation) -> String {
    match v {
        Violation::LostUpdate { id, detail } => format!("LostUpdate: {id}: {detail}"),
        Violation::Phantom { what } => format!("Phantom: {what}"),
        Violation::FalseConflict { id, landed_twice: false } => {
            format!("FalseConflict: {id} reported a failure but landed")
        }
        Violation::FalseConflict { id, landed_twice: true } => format!(
            "FalseConflict: {id} landed twice: its rebase conflicted with its own \
             first landing"
        ),
        Violation::BrokenHistory { detail } => format!("BrokenHistory: {detail}"),
        Violation::Stall { from_ms, to_ms } => format!(
            "Stall: no ack for {} ms, {} to {} UTC",
            to_ms - from_ms,
            time(*from_ms),
            time(*to_ms)
        ),
        Violation::FaultFloor { kind, fired, required } => {
            let remedy = if kind == EXTERNAL_KILL_FLOOR {
                "raise --duration or lower --external-kill-every"
            } else {
                "raise --duration or the kill/pause probability"
            };
            format!(
                "FaultFloor: {kind} fired {fired} times, at least {required} \
                 required; {remedy}"
            )
        }
        Violation::Recovery { detail } => format!("Recovery: {detail}"),
        Violation::Leftovers { names: leftover } => {
            format!(
                "Leftovers: {} in the storage root: {}",
                leftover.len(),
                names(leftover)
            )
        }
        Violation::DataMismatch { detail } => format!("DataMismatch: {detail}"),
        Violation::Harness { detail } => format!("Harness: {detail}"),
    }
}

fn warning(w: &Warning) -> String {
    match w {
        Warning::LostReplyConflict { id } => {
            format!("LostReplyConflict: {id} reported a conflict but landed")
        }
        Warning::AmbiguousErrorLanded { id } => {
            format!("AmbiguousErrorLanded: {id} reported an error but landed")
        }
        Warning::Stall { from_ms, to_ms } => format!(
            "Stall: no ack for {} ms, {} to {} UTC",
            to_ms - from_ms,
            time(*from_ms),
            time(*to_ms)
        ),
        Warning::StartSkew { ms } => format!("StartSkew: hosts saw the go {ms} ms apart"),
        Warning::Starved { tag, longest_gap_ms } => {
            format!("Starved: {tag} went {longest_gap_ms} ms without an ack")
        }
        Warning::SubdirStaging { count } => {
            format!("SubdirStaging: {count} staging files below the storage root")
        }
        Warning::Stragglers { tag, count } => {
            format!(
                "Stragglers: {tag} killed {count} workers still running at its deadline"
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::{
        config::{Backend, Mode},
        env_info::host_info,
        oracle::Stats,
    };

    fn every_violation() -> Vec<Violation> {
        let id = Uuid::new_v4();
        let detail = || "details".to_owned();
        vec![
            Violation::LostUpdate { id, detail: detail() },
            Violation::Phantom { what: "chain element e".to_owned() },
            Violation::FalseConflict { id, landed_twice: false },
            Violation::BrokenHistory { detail: detail() },
            Violation::Stall { from_ms: 1, to_ms: 40_000 },
            Violation::FaultFloor { kind: "pause".to_owned(), fired: 1, required: 3 },
            Violation::Recovery { detail: detail() },
            Violation::Leftovers { names: vec!["repo.new_bytes.x".to_owned()] },
            Violation::DataMismatch { detail: detail() },
            Violation::Harness { detail: detail() },
        ]
    }

    fn every_warning() -> Vec<Warning> {
        let id = Uuid::new_v4();
        vec![
            Warning::LostReplyConflict { id },
            Warning::AmbiguousErrorLanded { id },
            Warning::Stall { from_ms: 1, to_ms: 2 },
            Warning::StartSkew { ms: 3000 },
            Warning::Starved { tag: "h".to_owned(), longest_gap_ms: 9 },
            Warning::SubdirStaging { count: 4 },
            Warning::Stragglers { tag: "h".to_owned(), count: 1 },
        ]
    }

    fn report(violations: Vec<Violation>) -> Report {
        let dir = tempfile::tempdir().unwrap();
        let verdict = Verdict {
            violations,
            warnings: every_warning(),
            stats: Stats {
                intents: 10,
                acks: 7,
                faults: [("pause".to_owned(), 2)].into(),
                ..Stats::default()
            },
        };
        let done = HostDone {
            tag: "h".to_owned(),
            go_seen_at_ms: 1,
            stopped_at_ms: 2,
            workers: 8,
            incarnations_spawned: 9,
            external_kills: 1,
            stragglers: 0,
        };
        Report::new(
            verdict,
            RunConfig::defaults(Mode::Storage, 1),
            Participants { tags: ["h".to_owned()].into(), missing: vec![] },
            vec![host_info("h", dir.path())],
            vec![done],
            Some(1234),
            vec!["a note".to_owned()],
        )
    }

    #[test]
    fn report_round_trips_through_report_json() {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().to_path_buf());
        let report = report(every_violation());
        assert!(!report.passed);
        report.write(&root).unwrap();
        assert_eq!(Report::load(&root).unwrap(), report);
        let names: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, ["report.json"]);
    }

    #[test]
    fn summary_names_the_result_and_every_violation_and_warning_kind() {
        let failed = report(every_violation()).summary();
        assert!(failed.starts_with("FAIL"), "{failed}");
        for kind in [
            "LostUpdate",
            "Phantom",
            "FalseConflict",
            "BrokenHistory",
            "Stall",
            "FaultFloor",
            "Recovery",
            "Leftovers",
            "DataMismatch",
            "Harness",
            "LostReplyConflict",
            "AmbiguousErrorLanded",
            "StartSkew",
            "Starved",
            "SubdirStaging",
            "Stragglers",
        ] {
            assert!(failed.contains(kind), "{kind} missing from {failed}");
        }
        for expected in ["storage", "1234 ms", "60s", "10 intents", "pause 2", "a note"] {
            assert!(failed.contains(expected), "{expected} missing from {failed}");
        }

        let passed = report(vec![]).summary();
        assert!(passed.starts_with("PASS"), "{passed}");
    }

    #[test]
    fn summary_labels_a_non_default_backend() {
        let filesystem = report(vec![]);
        assert!(!filesystem.summary().contains("backend"), "{}", filesystem.summary());
        let mut legacy = report(vec![]);
        legacy.config.backend = Backend::LegacyLocal;
        let summary = legacy.summary();
        let first = summary.lines().next().unwrap();
        assert!(first.contains("backend: legacy-local"), "{summary}");
    }

    #[test]
    fn summary_shows_at_most_20_violations_and_the_total() {
        let many = (0..25)
            .map(|i| Violation::Phantom { what: format!("element {i}") })
            .collect();
        let summary = report(many).summary();
        assert!(summary.contains("violations (25)"), "{summary}");
        assert!(summary.contains("element 19") && !summary.contains("element 20"));
        assert!(summary.contains("5 more"), "{summary}");
    }

    #[test]
    fn fault_floor_violations_name_the_remedy() {
        let floor = |kind: &str| {
            let fired =
                Violation::FaultFloor { kind: kind.to_owned(), fired: 1, required: 3 };
            report(vec![fired]).summary()
        };
        let external = floor("external_kill");
        assert!(external.contains("lower --external-kill-every"), "{external}");
        for kind in ["in_window_kill", "pause", "per_point.fenced"] {
            let summary = floor(kind);
            assert!(summary.contains("the kill/pause probability"), "{summary}");
        }
    }

    #[test]
    fn summary_counts_the_participants() {
        let mut report = report(vec![]);
        report.participants.tags = ["a", "b"].map(str::to_owned).into();
        let summary = report.summary();
        assert!(summary.lines().next().unwrap().contains(" 2 host(s)"), "{summary}");
    }

    #[test]
    fn violations_with_hosts_missing_are_provisional() {
        let harness = Violation::Harness { detail: "missing done/".to_owned() };
        let mut report = report(vec![every_violation().remove(0), harness]);
        assert_eq!(report.exit_code(), 1);
        assert!(!report.summary().contains("provisional"));
        report.participants.missing = vec!["b".to_owned(), "c".to_owned()];
        assert_eq!(report.exit_code(), 3);
        let summary = report.summary();
        assert!(
            summary.contains(
                "provisional: hosts still missing (b, c); verify again once they finished"
            ),
            "{summary}"
        );
    }

    #[test]
    fn false_conflict_text_tells_a_relanded_commit_from_a_failure() {
        let id = Uuid::new_v4();
        let summary = |landed_twice| {
            report(vec![Violation::FalseConflict { id, landed_twice }]).summary()
        };
        let failed = summary(false);
        assert!(failed.contains("reported a failure but landed"), "{failed}");
        let twice = summary(true);
        assert!(twice.contains("landed twice"), "{twice}");
        assert!(!twice.contains("reported a failure"), "{twice}");
    }

    #[test]
    fn exit_codes_separate_failures_from_harness_errors() {
        assert_eq!(report(vec![]).exit_code(), 0);
        assert_eq!(report(every_violation()).exit_code(), 1);
        let harness = Violation::Harness { detail: "missing done/".to_owned() };
        assert_eq!(report(vec![harness]).exit_code(), 3);
    }
}
