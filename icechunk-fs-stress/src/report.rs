//! `report.json`, written by `verify`, and its human summary.

use std::{collections::BTreeSet, fmt::Write as _, fs, path::Path, time::Duration};

use anyhow::Context as _;
use chrono::DateTime;
use serde::{Deserialize, Serialize};

use crate::{
    config::{Backend, Mode, RunConfig},
    env_info::{Build, HostDone, HostInfo},
    layout::RunRoot,
    log::Direction,
    oracle::{
        Verdict, Violation, Warning,
        liveness::EXTERNAL_KILL_FLOOR,
        mirror::{MirrorCheckpoint, MirrorFound, MirrorSource},
    },
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub passed: bool,
    /// The binary that ran verify.
    pub build: Build,
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

/// What a report says about the filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Passed,
    /// Violations: the filesystem may be unsafe.
    Failed,
    /// Violations while hosts were still missing: they may still have been
    /// writing.
    Provisional,
    /// Only the harness failed: the run says nothing about the filesystem.
    Error,
}

impl Outcome {
    /// 0 for a pass, 1 for a failure, 3 for an error or a provisional
    /// verdict.
    pub fn exit_code(self) -> u8 {
        match self {
            Self::Passed => 0,
            Self::Failed => 1,
            Self::Provisional | Self::Error => 3,
        }
    }
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
            build: Build::current(),
            verdict,
            config,
            participants,
            hosts,
            done,
            recovery_ms,
            notes,
        }
    }

    /// Writes `report.json`, then `summary.txt` with the summary, replacing
    /// any earlier ones by a rename.
    pub fn write(&self, root: &RunRoot) -> anyhow::Result<()> {
        replace_file(&root.report_json(), &serde_json::to_vec_pretty(self)?)?;
        write_summary(root, &self.summary())
    }

    pub fn load(root: &RunRoot) -> anyhow::Result<Self> {
        let path = root.report_json();
        let bytes =
            fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {}", path.display()))
    }

    pub fn outcome(&self) -> Outcome {
        let violations = &self.verdict.violations;
        if violations.is_empty() {
            Outcome::Passed
        } else if self.is_provisional() {
            Outcome::Provisional
        } else if violations.iter().all(|v| matches!(v, Violation::Harness { .. })) {
            Outcome::Error
        } else {
            Outcome::Failed
        }
    }

    pub fn exit_code(&self) -> u8 {
        self.outcome().exit_code()
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
        writeln!(out, "build: {}", self.build)?;
        let other_builds: Vec<_> = self
            .hosts
            .iter()
            .filter(|host| host.build != self.build)
            .map(|host| format!("{} ran {}", host.tag, host.build))
            .collect();
        if !other_builds.is_empty() {
            writeln!(
                out,
                "note: some hosts ran a different build than verify: {}",
                other_builds.join("; ")
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
            "stats: {} intents, {} acks, {} conflicts, {} errors, {} refused, \
             {} unresolved, {} failed before intent; chain length {}; {} external kills",
            stats.intents,
            stats.acks,
            stats.conflicts,
            stats.errors,
            stats.refused,
            stats.unresolved,
            stats.failures,
            stats.chain_len,
            stats.external_kills
        )?;
        if stats.mirror_renames + stats.mirror_samples + stats.readonly_holds > 0 {
            writeln!(
                out,
                "mirror: {} renames, {} sample changes; {} ReadOnly holds, {} ended with \
                 mirror lag, {} not judged after a storage error",
                stats.mirror_renames,
                stats.mirror_samples,
                stats.readonly_holds,
                stats.readonly_mirror_lags,
                stats.readonly_publish_errors
            )?;
        }
        if !stats.faults.is_empty() {
            let faults: Vec<_> =
                stats.faults.iter().map(|(kind, n)| format!("{kind} {n}")).collect();
            writeln!(out, "faults: {}", faults.join(", "))?;
        }
        if !stats.observed.is_empty() {
            let observed: Vec<_> =
                stats.observed.iter().map(|(what, n)| format!("{what} {n}")).collect();
            writeln!(out, "observed: {}", observed.join(", "))?;
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

/// Writes `summary.txt`, replacing any earlier one by a rename: for a
/// command that prints more than [`Report::summary`].
pub fn write_summary(root: &RunRoot, text: &str) -> anyhow::Result<()> {
    replace_file(&root.summary(), text.as_bytes())
}

/// Writes `path` through a staging file next to it, renamed over it.
pub fn replace_file(path: &Path, contents: &[u8]) -> anyhow::Result<()> {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let staging = path.with_file_name(format!("{name}.{}.tmp", uuid::Uuid::new_v4()));
    let written = fs::write(&staging, contents).and_then(|()| fs::rename(&staging, path));
    if written.is_err() {
        // Best effort: the error being reported is the write's.
        let _ = fs::remove_file(&staging);
    }
    written.with_context(|| format!("writing {}", path.display()))
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
        Violation::MirrorNotAVersion { hash, source, t_ms } => format!(
            "MirrorNotAVersion: {} at {} UTC installed {hash}, never a current version",
            match source {
                MirrorSource::Rename => "a mirror rename",
                MirrorSource::Sample => "a sample of repo",
            },
            time(*t_ms)
        ),
        Violation::MirrorStale { expected, found, after } => {
            mirror_stale(expected, found, *after)
        }
        Violation::MirrorDangling { detail } => format!("MirrorDangling: {detail}"),
        Violation::FeatureMissing { what, detail } => {
            format!("FeatureMissing: {what}: {detail}")
        }
        Violation::ReadOnlyMirrorMismatch { version, detail } => {
            readonly_mirror_mismatch(version.as_deref(), detail)
        }
        Violation::DeletedUnrecognized { name } => format!(
            "DeletedUnrecognized: the sweep or GC deleted {name}, which it doesn't recognize"
        ),
        Violation::InitialVersion { n, t_before_ms, t_after_ms } => format!(
            "InitialVersion: the first version's N {n} isn't within its creation's \
             [{t_before_ms}, {t_after_ms}] ms"
        ),
        Violation::UnreportedFork { id, op } => unreported_fork(*id, op),
        Violation::GcAfterFork { id } => gc_after_fork(*id),
        Violation::SilentForkLoss { fork_hash, erased_by } => format!(
            "SilentForkLoss: the fork {fork_hash} was erased outside the accepted window, \
             by {erased_by}"
        ),
        Violation::ForkDamaged { detail } => format!("ForkDamaged: {detail}"),
        Violation::ConversionMismatch { direction, detail } => {
            format!("ConversionMismatch: {}: {detail}", conversion(*direction))
        }
        Violation::ConversionEndState { direction, detail } => {
            format!("ConversionEndState: {}: {detail}", conversion(*direction))
        }
        Violation::OverwroteStandard { detail } => format!("OverwroteStandard: {detail}"),
        Violation::DeletedHistory { detail } => format!("DeletedHistory: {detail}"),
        Violation::WriterDuringConversion { id, detail } => format!(
            "WriterDuringConversion: {id}, during an interrupted conversion to the standard \
             layout: {detail}"
        ),
        Violation::ConversionRetried { direction, detail } => {
            conversion_retried(*direction, detail)
        }
    }
}

fn unreported_fork(id: uuid::Uuid, op: &str) -> String {
    format!("UnreportedFork: {op} {id} landed after a fork and was acknowledged")
}

fn gc_after_fork(id: uuid::Uuid) -> String {
    format!("GcAfterFork: GC {id} deleted with a fork in repo and was acknowledged")
}

fn conversion(direction: Direction) -> &'static str {
    match direction {
        Direction::FromStandard => "convert_to_filesystem_layout()",
        Direction::ToStandard => "convert_to_standard_layout()",
    }
}

fn conversion_retried(direction: Direction, detail: &str) -> String {
    format!(
        "ConversionRetried: a rerun of {} failed as under stale listings and was \
         retried: {detail}",
        conversion(direction)
    )
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
        Warning::AttemptFailures { count, samples } => format!(
            "AttemptFailures: {count} attempts failed reading or backing up repo, \
             before their intent; first: {}",
            samples.join("; ")
        ),
        Warning::MirrorStale { expected, found, after } => {
            mirror_stale(expected, found, *after)
        }
        Warning::ReadOnlyMirrorMismatch { version, detail } => {
            readonly_mirror_mismatch(version.as_deref(), detail)
        }
        Warning::UnknownEntries { names: unknown } => format!(
            "UnknownEntries: {} the layout never creates: {}",
            unknown.len(),
            names(unknown)
        ),
        Warning::SamplerFailed { tag, error } => {
            format!("SamplerFailed: {tag}'s mirror sampler stopped logging: {error}")
        }
        Warning::MirrorMissing { count } => {
            format!("MirrorMissing: {count} samples found no repo")
        }
        Warning::MirrorUnreadable { count, samples } => format!(
            "MirrorUnreadable: {count} samples couldn't read repo; first: {}",
            samples.join("; ")
        ),
        Warning::UnreportedFork { id, op } => unreported_fork(*id, op),
        Warning::GcAfterFork { id } => gc_after_fork(*id),
        Warning::ForkErasedInWindow { fork_hash, landed_ms_after_check } => {
            let landed = match landed_ms_after_check {
                Some(ms) => {
                    format!("its rename returning {ms} ms after the check passed")
                }
                None => "whose rename was announced but never recorded done".to_owned(),
            };
            format!(
                "ForkErasedInWindow: the fork {fork_hash} was erased by a publication whose \
                 fork check read repo before it (its last read began by the end of the \
                 fork's write, or, in logs without the read's time, it passed by then), \
                 {landed}"
            )
        }
        Warning::ForkErasedAfterKilledPublisher { fork_hash, replaced_by } => format!(
            "ForkErasedAfterKilledPublisher: the fork {fork_hash} was erased by \
             {replaced_by}, which a publication whose fork check read repo before it \
             announced renaming, its process seen dead before the fork, and which no \
             publication checked after the fork, or unchecked, may have renamed since: \
             the server may apply a killed process's rename after its death"
        ),
        Warning::ConversionRetried { direction, detail } => {
            conversion_retried(*direction, detail)
        }
        Warning::StaleRootListing { attempt, rerun_after_ms } => format!(
            "StaleRootListing: run {attempt} of convert_to_standard_layout() returned \
             NothingToConvert and left repo.d, with repo its current version without the \
             feature: its listing of the storage root missed repo.d (an accepted limit); \
             running it again after {rerun_after_ms} ms"
        ),
        Warning::ForkActorPartialBackup { backup, detail } => format!(
            "ForkActorPartialBackup: overwritten/{backup}, announced by a fork's write \
             that wrote no fork naming it, doesn't decode: an interrupted copy no ops log \
             references: {detail}"
        ),
        Warning::GcCutoffAfterSessionStart {
            id,
            snapshot,
            intent_ms,
            outcome_ms,
            gc,
            gc_intent_ms,
            cutoff_ms,
            missing,
        } => gc_cutoff_after_session_start(
            *id,
            snapshot.as_deref(),
            (*intent_ms, *outcome_ms),
            (*gc, *gc_intent_ms, *cutoff_ms),
            missing,
        ),
    }
}

fn gc_cutoff_after_session_start(
    id: uuid::Uuid,
    snapshot: Option<&str>,
    (intent_ms, outcome_ms): (i64, Option<i64>),
    (gc, gc_intent_ms, cutoff_ms): (uuid::Uuid, i64, i64),
    missing: &[String],
) -> String {
    let ended = outcome_ms.map_or_else(
        || "logged no outcome".to_owned(),
        |t_ms| format!("ended {} ms after the GC began", t_ms - gc_intent_ms),
    );
    let snapshot = snapshot
        .map_or_else(|| "no acked snapshot".to_owned(), |s| format!("acked as {s}"));
    let missing = if missing.is_empty() { "none".to_owned() } else { names(missing) };
    format!(
        "GcCutoffAfterSessionStart: commit {id} started at {} UTC, {} ms before the cutoff \
         of GC {gc}, which began at {} UTC; the commit {ended}, {snapshot}. GC's \
         precondition was broken (the cutoff must be earlier than the start of every \
         session that may still commit), so GC may have deleted the commit's objects; its \
         missing snapshot file and transaction log, and the read-back of its chunk \
         finding the manifest or chunk it wrote missing, are excused: {missing} (a \
         missing manifest is still MirrorDangling)",
        time(intent_ms),
        cutoff_ms - intent_ms,
        time(gc_intent_ms),
    )
}

fn readonly_mirror_mismatch(version: Option<&str>, detail: &str) -> String {
    let version = version.unwrap_or("an unknown version");
    format!(
        "ReadOnlyMirrorMismatch: at the end of the ReadOnly hold of {version}, {detail}"
    )
}

fn mirror_stale(expected: &str, found: &MirrorFound, after: MirrorCheckpoint) -> String {
    let after = match after {
        MirrorCheckpoint::FinalPublish => "verify's publish_mirror()",
        MirrorCheckpoint::RecoveryWrite => "the recovery write",
    };
    let found = match found {
        MirrorFound::Hash(hash) => format!("repo is {hash}"),
        MirrorFound::Missing => "repo is missing".to_owned(),
        MirrorFound::Unreadable(err) => format!("repo is unreadable ({err})"),
        MirrorFound::PublishFailed(err) => format!("publish_mirror() failed ({err})"),
    };
    format!("MirrorStale: after {after}, {found}, not the current version {expected}")
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
            Violation::Leftovers { names: vec!["repo.d/stage.x".to_owned()] },
            Violation::DataMismatch { detail: detail() },
            Violation::Harness { detail: detail() },
            Violation::MirrorNotAVersion {
                hash: "h".to_owned(),
                source: MirrorSource::Rename,
                t_ms: 5,
            },
            Violation::MirrorNotAVersion {
                hash: "h".to_owned(),
                source: MirrorSource::Sample,
                t_ms: 5,
            },
            Violation::MirrorStale {
                expected: "a".to_owned(),
                found: MirrorFound::Hash("b".to_owned()),
                after: MirrorCheckpoint::FinalPublish,
            },
            Violation::MirrorDangling { detail: detail() },
            Violation::FeatureMissing { what: "the mirror".to_owned(), detail: detail() },
            Violation::InitialVersion { n: 1, t_before_ms: 2, t_after_ms: 3 },
            Violation::ReadOnlyMirrorMismatch {
                version: Some("v".to_owned()),
                detail: detail(),
            },
            Violation::DeletedUnrecognized { name: "chunks/x".to_owned() },
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
            Warning::AttemptFailures { count: 2, samples: vec!["boom".to_owned()] },
            Warning::MirrorStale {
                expected: "a".to_owned(),
                found: MirrorFound::Missing,
                after: MirrorCheckpoint::RecoveryWrite,
            },
            Warning::ReadOnlyMirrorMismatch { version: None, detail: "d".to_owned() },
            Warning::UnknownEntries { names: vec!["repo.legacy.x".to_owned()] },
            Warning::MirrorMissing { count: 1 },
            Warning::SamplerFailed { tag: "h".to_owned(), error: "e".to_owned() },
            Warning::MirrorUnreadable { count: 1, samples: vec!["ESTALE".to_owned()] },
            Warning::StaleRootListing { attempt: 1, rerun_after_ms: 500 },
            Warning::ForkActorPartialBackup {
                backup: "repo.1.X".to_owned(),
                detail: "d".to_owned(),
            },
            Warning::GcCutoffAfterSessionStart {
                id,
                snapshot: Some("S".to_owned()),
                intent_ms: 1,
                outcome_ms: Some(9),
                gc: id,
                gc_intent_ms: 5,
                cutoff_ms: 3,
                missing: vec!["transactions/S".to_owned()],
            },
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
                observed: [("rename_enoent.landed".to_owned(), 3)].into(),
                refused: 4,
                mirror_renames: 5,
                mirror_samples: 6,
                readonly_holds: 2,
                readonly_mirror_lags: 1,
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
            sampler_error: None,
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
    fn report_round_trips_through_report_json_next_to_its_summary() {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().to_path_buf());
        let report = report(every_violation());
        assert!(!report.passed);
        report.write(&root).unwrap();
        assert_eq!(Report::load(&root).unwrap(), report);
        assert_eq!(fs::read_to_string(root.summary()).unwrap(), report.summary());
        let mut names: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, ["report.json", "summary.txt"]);

        write_summary(&root, "printed\n").unwrap();
        assert_eq!(fs::read_to_string(root.summary()).unwrap(), "printed\n");
    }

    #[test]
    fn summary_shows_the_build_after_the_result() {
        let report = report(vec![]);
        let summary = report.summary();
        let second = summary.lines().nth(1).unwrap();
        assert_eq!(second, format!("build: {}", Build::current()), "{summary}");
        assert!(!summary.contains("different build"), "{summary}");
    }

    #[test]
    fn hosts_on_another_build_than_verify_are_noted() {
        let mut report = report(vec![]);
        let mut other = report.hosts[0].clone();
        other.tag = "g".to_owned();
        other.build.commit = "0123abc".to_owned();
        report.hosts.push(other);
        let summary = report.summary();
        assert!(summary.starts_with("PASS"), "{summary}");
        let expected = format!(
            "note: some hosts ran a different build than verify: g ran {}",
            report.hosts[1].build
        );
        assert!(summary.contains(&expected), "{expected} missing from {summary}");
        assert!(!summary.contains("h ran"), "{summary}");
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
            "AttemptFailures",
            "MirrorNotAVersion",
            "MirrorStale",
            "MirrorDangling",
            "FeatureMissing",
            "InitialVersion",
            "ReadOnlyMirrorMismatch",
            "DeletedUnrecognized",
            "UnknownEntries",
            "MirrorMissing",
            "MirrorUnreadable",
            "SamplerFailed",
            "StaleRootListing",
            "ForkActorPartialBackup",
            "GcCutoffAfterSessionStart",
        ] {
            assert!(failed.contains(kind), "{kind} missing from {failed}");
        }
        for expected in [
            "storage",
            "1234 ms",
            "60s",
            "10 intents",
            "4 refused",
            "mirror: 5 renames, 6 sample changes; 2 ReadOnly holds, 1 ended with mirror lag, \
             0 not judged after a storage error",
            "pause 2",
            "rename_enoent.landed 3",
            "a note",
            "2 ms before the cutoff",
            "ended 4 ms after the GC began, acked as S",
            "excused: transactions/S",
        ] {
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
        for kind in ["in_window_kill", "pause", "per_point.before_rename"] {
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
        assert_eq!(report.outcome(), Outcome::Provisional);
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
        assert_eq!(report(vec![]).outcome(), Outcome::Passed);
        assert_eq!(report(vec![]).exit_code(), 0);
        assert_eq!(report(every_violation()).outcome(), Outcome::Failed);
        assert_eq!(report(every_violation()).exit_code(), 1);
        let harness = Violation::Harness { detail: "missing done/".to_owned() };
        assert_eq!(report(vec![harness.clone()]).outcome(), Outcome::Error);
        assert_eq!(report(vec![harness]).exit_code(), 3);
    }
}
