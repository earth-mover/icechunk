//! JSON-lines logs written by workers and supervisors under `logs/`.
//!
//! Each record is one line, written by a single `write_all` to a file opened
//! with `O_APPEND` and never fsynced. A process killed mid-write leaves a torn
//! last line, which readers skip.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read as _, Seek as _, SeekFrom, Write as _},
    path::Path,
    sync::{Mutex, PoisonError},
};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    config::{PointName, WorkerId},
    layout::{RunRoot, SAMPLER_SLOT, SUPERVISOR_SLOT, is_valid_slot, is_valid_tag},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Record {
    Start {
        t_ms: i64,
        host: String,
        slot: String,
        incarnation: u32,
        pid: u32,
    },
    Intent {
        t_ms: i64,
        id: Uuid,
        op: Op,
    },
    Outcome {
        t_ms: i64,
        id: Uuid,
        result: OpResult,
    },
    /// Storage mode: an attempt that failed reading or backing up `repo`,
    /// before its intent.
    Failure {
        t_ms: i64,
        message: String,
    },
    /// A doubtful outcome a commit resolved, named as in
    /// [`observed_name`](crate::faults::observed_name).
    Observed {
        t_ms: i64,
        what: String,
    },
    /// A `Kill` is written before the kill.
    Fault {
        t_ms: i64,
        action: FaultAction,
        point: PointName,
    },
    /// Supervisor log only.
    Spawn {
        t_ms: i64,
        slot: String,
        incarnation: u32,
        pid: u32,
    },
    /// Supervisor log only.
    ExternalKill {
        t_ms: i64,
        slot: String,
        incarnation: u32,
    },
    /// A publication is about to rename its staged copy, hashing to `hash`,
    /// over `$ROOT/repo`. A [`Record::MirrorRename`] follows if the rename
    /// succeeded; if this is the log's last record, the process died and the
    /// rename may have happened.
    MirrorRenaming {
        t_check_ms: Option<i64>,
        /// As in [`Record::MirrorRename`].
        t_read_ms: Option<i64>,
        t_ms: i64,
        hash: String,
    },
    /// A publication renamed its staged copy over `$ROOT/repo`: `hash` is the
    /// sha256 of the staged bytes, `t_check_ms` the time its fork check
    /// passed (the last `BeforeMirrorRename` point on the renaming thread),
    /// `t_read_ms` the time that check's last read of `$ROOT/repo` began
    /// (the last `BeforeMirrorRead` point of the same round), `None` if it
    /// read nothing or in logs from before it was logged.
    MirrorRename {
        t_check_ms: Option<i64>,
        t_read_ms: Option<i64>,
        t_rename_ms: i64,
        hash: String,
    },
    /// What a read of `$ROOT/repo` found, logged when it changes.
    MirrorSample {
        t_ms: i64,
        sample: Sample,
    },
    /// The first version of `repo.d` that `init` created: `n` must lie in
    /// `[t_before_ms, t_after_ms]`.
    InitVersion {
        n: u64,
        t_before_ms: i64,
        t_after_ms: i64,
    },
    /// The status toggler held the status `ReadOnly`: its change returned at
    /// `t_ack_ms` having made the version `version`, `None` if the current
    /// version read right after wasn't that change's (another change
    /// landed); `publish_error` if the storage logged an error during the
    /// change, as a failed publication does. At `t_end_ms`, before setting
    /// the status `Online` again, the current version was `current` and a
    /// read of `$ROOT/repo` found `mirror`.
    ReadOnlyHeld {
        t_ack_ms: i64,
        version: Option<String>,
        publish_error: bool,
        t_end_ms: i64,
        current: String,
        mirror: Sample,
    },
    /// A fork's write is about to copy `$ROOT/repo` to `overwritten/<backup>`,
    /// as the old writer it emulates backs up: if the copy is interrupted,
    /// the backup may be partial, and no ops log names it.
    ForkBackup {
        t_ms: i64,
        backup: String,
    },
    /// A fork was written between `t_before_ms` and `t_after_ms`: `$ROOT/repo`
    /// held `base_hash`, was backed up to `overwritten/<backup>` and replaced
    /// by `fork_hash`, a repo info without the writer feature. `gc` is the
    /// GC whose change's publication wrote it, before that GC's hook.
    ForkWritten {
        t_before_ms: i64,
        t_after_ms: i64,
        base_hash: String,
        fork_hash: String,
        backup: String,
        gc: Option<Uuid>,
    },
    /// `$ROOT/repo` no longer held the fork `fork_hash` at `t_ms`: it held
    /// `found` instead.
    ForkGone {
        t_ms: i64,
        fork_hash: String,
        found: Sample,
    },
    /// The conversion scenario's converter: its conversion returned
    /// `NothingToConvert`, logged as an `Ack` right after this.
    NothingToConvert {
        t_ms: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sample {
    /// The sha256 of the bytes read.
    Hash(String),
    Missing,
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Op {
    /// Storage mode; `base` and `new` are etags, `backup` the path, relative
    /// to `overwritten/`, of the attempt's backup of `base`.
    Cas {
        base: String,
        new: String,
        backup: String,
    },
    BranchCommit {
        branch: String,
    },
    MainCommit,
    Tag {
        name: String,
        snapshot: String,
    },
    CreateBranch {
        branch: String,
    },
    /// Collects what is unreachable and older than its cutoff `older_than_ms`,
    /// `None` in logs from before it was logged.
    Gc {
        older_than_ms: Option<i64>,
    },
    /// Expires the snapshots flushed before `older_than_ms`, keeping every
    /// ref.
    Expire {
        older_than_ms: i64,
    },
    DeleteTag {
        name: String,
    },
    DeleteBranch {
        branch: String,
    },
    SetStatus {
        availability: Availability,
    },
    /// The conversion scenario's converter: an `Ack` is `Converted`, or
    /// `NothingToConvert` after a [`Record::NothingToConvert`].
    Convert {
        direction: Direction,
    },
}

/// The direction of a conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// `convert_to_filesystem_layout()`.
    FromObjectStore,
    /// `convert_to_object_store_layout()`.
    ToObjectStore,
}

/// The repository statuses the status toggler sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Availability {
    Online,
    ReadOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OpResult {
    Ack {
        snapshot: Option<String>,
    },
    Conflict,
    Error {
        message: String,
    },
    /// The repository's status refused the operation.
    ReadOnly {
        message: String,
    },
    /// The storage found a fork before changing anything.
    Forked {
        message: String,
    },
    /// The change landed and is durable, but its publication found a fork;
    /// `snapshot` is a commit's.
    DurableButForked {
        snapshot: Option<String>,
        message: String,
    },
    /// The current version lacks the layout's writer feature: a conversion
    /// to the object store layout was interrupted.
    ConversionInProgress {
        message: String,
    },
}

impl OpResult {
    /// `Some` with the commit's snapshot if the operation is known to have
    /// landed: acknowledged, or durable though its publication found a fork.
    pub fn landed(&self) -> Option<Option<&str>> {
        match self {
            Self::Ack { snapshot } | Self::DurableButForked { snapshot, .. } => {
                Some(snapshot.as_deref())
            }
            _ => None,
        }
    }

    /// The repository's status or layout refused the operation before it
    /// changed anything, unless an earlier attempt's reply was lost.
    pub fn is_refusal(&self) -> bool {
        matches!(
            self,
            Self::ReadOnly { .. }
                | Self::Forked { .. }
                | Self::ConversionInProgress { .. }
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultAction {
    Kill,
    Pause,
    Sabotage,
}

/// Wall-clock Unix milliseconds.
pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[derive(Debug)]
pub struct LogWriter {
    file: Mutex<File>,
}

impl LogWriter {
    /// Creates the log, which must not exist.
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().append(true).create_new(true).open(path)?;
        Ok(Self { file: Mutex::new(file) })
    }

    pub fn append(&self, record: &Record) -> io::Result<()> {
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        // A panic while holding the lock cannot leave a partial line from
        // `write_all` behind for us to extend, so a poisoned lock is usable.
        self.file.lock().unwrap_or_else(PoisonError::into_inner).write_all(&line)
    }
}

/// Creates a worker's log and writes its `Start` record.
pub fn start_worker_log(root: &RunRoot, who: &WorkerId) -> anyhow::Result<LogWriter> {
    let path = root.worker_log(&who.tag, &who.slot, who.incarnation);
    let log = LogWriter::create(&path)
        .with_context(|| format!("creating {}", path.display()))?;
    log.append(&Record::Start {
        t_ms: now_ms(),
        host: who.tag.clone(),
        slot: who.slot.clone(),
        incarnation: who.incarnation,
        pid: std::process::id(),
    })
    .with_context(|| format!("writing {}", path.display()))?;
    Ok(log)
}

/// Reads a log, skipping a torn last line. Any other unparsable line is an
/// error.
pub fn read_log(path: &Path) -> anyhow::Result<Vec<Record>> {
    let bytes =
        read_log_from(path, 0).with_context(|| format!("reading {}", path.display()))?;
    let mut lines: Vec<&[u8]> = bytes.split(|&b| b == b'\n').collect();
    // The segment after the last newline: empty, or a torn write.
    lines.pop();
    lines
        .into_iter()
        .enumerate()
        .map(|(i, line)| {
            serde_json::from_slice(line)
                .with_context(|| format!("{} line {}", path.display(), i + 1))
        })
        .collect()
}

/// The complete lines appended to a log since some offset.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Appended {
    pub records: Vec<Record>,
    pub malformed_lines: u64,
    /// Where the next read starts: after the last complete line.
    pub offset: u64,
}

/// Reads the records appended to `path` from byte `offset` on, without
/// rereading earlier ones. A torn last line is left for the next call; a
/// missing log has nothing appended yet.
pub fn read_appended(path: &Path, offset: u64) -> io::Result<Appended> {
    let bytes = match read_log_from(path, offset) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Ok(Appended { offset, ..Appended::default() });
        }
        Err(err) => return Err(err),
    };
    let complete = bytes.iter().rposition(|&b| b == b'\n').map_or(0, |last| last + 1);
    let mut appended = Appended {
        offset: offset + u64::try_from(complete).unwrap_or(0),
        ..Appended::default()
    };
    for line in bytes[..complete].split(|&b| b == b'\n').filter(|line| !line.is_empty()) {
        match serde_json::from_slice(line) {
            Ok(record) => appended.records.push(record),
            Err(_) => appended.malformed_lines += 1,
        }
    }
    Ok(appended)
}

/// A log's bytes from `offset` on. An empty log isn't opened: an `NFSv3`
/// exclusive create ([`LogWriter::create`]) killed before its follow-up
/// `SETATTR` leaves an empty log with mode 000, which can't be opened.
fn read_log_from(path: &Path, offset: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    if fs::metadata(path)?.len() > 0 {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.read_to_end(&mut bytes)?;
    }
    Ok(bytes)
}

/// Identifies a log file: `<tag>.<slot>.<incarnation>.log` for a worker,
/// `<tag>.supervisor.log` and `<tag>.sampler.log` (with `incarnation: None`)
/// for a supervisor and its mirror sampler.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogName {
    pub tag: String,
    pub slot: String,
    pub incarnation: Option<u32>,
}

impl LogName {
    pub fn parse(file_name: &str) -> Option<Self> {
        let (rest, last) = file_name.strip_suffix(".log")?.rsplit_once('.')?;
        let (tag, slot, incarnation) = if [SUPERVISOR_SLOT, SAMPLER_SLOT].contains(&last)
        {
            (rest, last, None)
        } else {
            let (tag, slot) = rest.rsplit_once('.')?;
            if !is_valid_slot(slot) {
                return None;
            }
            (tag, slot, Some(parse_canonical_u32(last)?))
        };
        is_valid_tag(tag).then(|| Self {
            tag: tag.to_owned(),
            slot: slot.to_owned(),
            incarnation,
        })
    }
}

/// Only the form [`RunRoot::worker_log`] writes: no sign, no leading zeros.
fn parse_canonical_u32(s: &str) -> Option<u32> {
    s.parse().ok().filter(|n: &u32| n.to_string() == s)
}

/// Reads every log under `logs/`, which must contain nothing else.
pub fn read_all_logs(root: &RunRoot) -> anyhow::Result<BTreeMap<LogName, Vec<Record>>> {
    let dir = root.logs_dir();
    let context = || format!("listing {}", dir.display());
    fs::read_dir(&dir)
        .with_context(context)?
        .map(|entry| {
            let path = entry.with_context(context)?.path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(LogName::parse)
                .with_context(|| format!("unexpected file {}", path.display()))?;
            Ok((name, read_log(&path)?))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_variant() -> Vec<Record> {
        let id = Uuid::new_v4();
        let host = "h-1".to_owned();
        let slot = "3".to_owned();
        let op = |op| Record::Intent { t_ms: 2, id, op };
        let outcome = |result| Record::Outcome { t_ms: 3, id, result };
        vec![
            Record::Start { t_ms: 1, host, slot: slot.clone(), incarnation: 4, pid: 77 },
            op(Op::Cas {
                base: "aa".to_owned(),
                new: "bb".to_owned(),
                backup: "repo.1.X".to_owned(),
            }),
            op(Op::BranchCommit { branch: "w.h-1.3".to_owned() }),
            op(Op::MainCommit),
            op(Op::Tag { name: format!("t-{id}"), snapshot: "SNAP".to_owned() }),
            op(Op::CreateBranch { branch: "w.h-1.3".to_owned() }),
            op(Op::Gc { older_than_ms: Some(12) }),
            op(Op::Gc { older_than_ms: None }),
            outcome(OpResult::Ack { snapshot: Some("SNAP".to_owned()) }),
            outcome(OpResult::Ack { snapshot: None }),
            outcome(OpResult::Conflict),
            outcome(OpResult::Error { message: "boom\nline".to_owned() }),
            Record::Fault {
                t_ms: 4,
                action: FaultAction::Kill,
                point: PointName::Linked,
            },
            Record::Fault {
                t_ms: 4,
                action: FaultAction::Pause,
                point: PointName::BeforeRename,
            },
            Record::Fault {
                t_ms: 4,
                action: FaultAction::Sabotage,
                point: PointName::BeforeTidyUp,
            },
            Record::Spawn { t_ms: 5, slot: slot.clone(), incarnation: 5, pid: 78 },
            Record::ExternalKill { t_ms: 6, slot, incarnation: 5 },
            Record::MirrorRename {
                t_check_ms: Some(7),
                t_read_ms: Some(6),
                t_rename_ms: 8,
                hash: "cc".to_owned(),
            },
            Record::MirrorRename {
                t_check_ms: None,
                t_read_ms: None,
                t_rename_ms: 8,
                hash: "cc".to_owned(),
            },
            Record::MirrorSample { t_ms: 9, sample: Sample::Hash("dd".to_owned()) },
            Record::MirrorSample { t_ms: 9, sample: Sample::Error("ESTALE".to_owned()) },
            Record::MirrorSample { t_ms: 9, sample: Sample::Missing },
            Record::InitVersion { n: 10, t_before_ms: 9, t_after_ms: 11 },
            op(Op::Expire { older_than_ms: 12 }),
            op(Op::DeleteTag { name: format!("t-{id}") }),
            op(Op::DeleteBranch { branch: "w.h-1.3".to_owned() }),
            op(Op::SetStatus { availability: Availability::ReadOnly }),
            op(Op::SetStatus { availability: Availability::Online }),
            outcome(OpResult::ReadOnly { message: "held".to_owned() }),
            outcome(OpResult::Forked { message: "fork".to_owned() }),
            outcome(OpResult::DurableButForked {
                snapshot: Some("SNAP".to_owned()),
                message: "fork".to_owned(),
            }),
            outcome(OpResult::DurableButForked {
                snapshot: None,
                message: "fork".to_owned(),
            }),
            outcome(OpResult::ConversionInProgress { message: "rerun".to_owned() }),
            Record::MirrorRenaming {
                t_check_ms: Some(7),
                t_read_ms: Some(6),
                t_ms: 8,
                hash: "cc".to_owned(),
            },
            Record::ReadOnlyHeld {
                t_ack_ms: 13,
                version: Some("ee".to_owned()),
                publish_error: false,
                t_end_ms: 14,
                current: "ee".to_owned(),
                mirror: Sample::Hash("ee".to_owned()),
            },
            Record::ForkBackup { t_ms: 15, backup: "repo.1.X".to_owned() },
            Record::ForkWritten {
                t_before_ms: 15,
                t_after_ms: 16,
                base_hash: "ee".to_owned(),
                fork_hash: "ff".to_owned(),
                backup: "repo.1.X".to_owned(),
                gc: Some(id),
            },
            Record::ForkGone {
                t_ms: 17,
                fork_hash: "ff".to_owned(),
                found: Sample::Missing,
            },
            op(Op::Convert { direction: Direction::FromObjectStore }),
            op(Op::Convert { direction: Direction::ToObjectStore }),
            Record::NothingToConvert { t_ms: 18 },
        ]
    }

    #[test]
    fn append_then_read_round_trips_every_variant() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.0.0.log");
        let writer = LogWriter::create(&path).unwrap();
        let records = every_variant();
        for record in &records {
            writer.append(record).unwrap();
        }
        assert_eq!(read_log(&path).unwrap(), records);
        assert!(LogWriter::create(&path).is_err());
    }

    #[test]
    fn a_gc_intent_without_its_cutoff_parses() {
        let id = Uuid::new_v4();
        let line =
            format!(r#"{{"kind":"intent","t_ms":1,"id":"{id}","op":{{"kind":"gc"}}}}"#);
        let record: Record = serde_json::from_str(&line).unwrap();
        assert!(
            matches!(record, Record::Intent { op: Op::Gc { older_than_ms: None }, .. }),
            "{record:?}"
        );
    }

    #[test]
    fn mirror_renames_without_their_read_time_parse() {
        let lines = [
            r#"{"kind":"mirror_renaming","t_check_ms":7,"t_ms":8,"hash":"cc"}"#,
            r#"{"kind":"mirror_rename","t_check_ms":7,"t_rename_ms":9,"hash":"cc"}"#,
        ];
        let records: Vec<Record> =
            lines.iter().map(|line| serde_json::from_str(line).unwrap()).collect();
        assert_eq!(
            records,
            [
                Record::MirrorRenaming {
                    t_check_ms: Some(7),
                    t_read_ms: None,
                    t_ms: 8,
                    hash: "cc".to_owned()
                },
                Record::MirrorRename {
                    t_check_ms: Some(7),
                    t_read_ms: None,
                    t_rename_ms: 9,
                    hash: "cc".to_owned()
                },
            ]
        );
    }

    #[test]
    fn torn_last_line_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.0.0.log");
        let writer = LogWriter::create(&path).unwrap();
        let record = Record::Start {
            t_ms: 1,
            host: "h".to_owned(),
            slot: "0".to_owned(),
            incarnation: 0,
            pid: 1,
        };
        writer.append(&record).unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"kind\":\"fau").unwrap();
        assert_eq!(read_log(&path).unwrap(), std::slice::from_ref(&record));

        // Even a parseable last line is torn without its newline.
        fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();
        assert_eq!(read_log(&path).unwrap(), []);

        fs::write(&path, b"").unwrap();
        assert_eq!(read_log(&path).unwrap(), []);
    }

    /// What an `NFSv3` exclusive create killed before its `SETATTR` leaves.
    #[test]
    fn unreadable_log_is_empty_only_if_it_has_no_bytes() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.0.0.log");
        fs::write(&path, b"").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
        if File::open(&path).is_ok() {
            // Privileged (root): permissions don't stop the open.
            return;
        }
        assert_eq!(read_log(&path).unwrap(), []);
        assert_eq!(read_appended(&path, 0).unwrap(), Appended::default());

        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&path, b"{}\n").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
        let err = format!("{:#}", read_log(&path).unwrap_err());
        assert!(err.contains("Permission denied"), "{err}");
        let err = read_appended(&path, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn corrupt_middle_line_names_file_and_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.0.0.log");
        let good = serde_json::to_string(&Record::ExternalKill {
            t_ms: 1,
            slot: "0".to_owned(),
            incarnation: 0,
        })
        .unwrap();
        fs::write(&path, format!("{good}\n{{\"kind\":\"nope\"}}\n{good}\n")).unwrap();
        let err = format!("{:#}", read_log(&path).unwrap_err());
        assert!(err.contains(&path.display().to_string()), "{err}");
        assert!(err.contains("line 2"), "{err}");

        fs::write(&path, format!("{good}\n\n{good}\n")).unwrap();
        let err = format!("{:#}", read_log(&path).unwrap_err());
        assert!(err.contains("line 2"), "{err}");
    }

    #[test]
    fn read_appended_reads_each_complete_line_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.0.0.log");
        assert_eq!(read_appended(&path, 0).unwrap(), Appended::default());

        let writer = LogWriter::create(&path).unwrap();
        let records = every_variant();
        writer.append(&records[0]).unwrap();
        writer.append(&records[1]).unwrap();
        let first = read_appended(&path, 0).unwrap();
        assert_eq!(first.records, records[..2]);
        assert_eq!(first.malformed_lines, 0);
        assert_eq!(first.offset, fs::metadata(&path).unwrap().len());

        let again = read_appended(&path, first.offset).unwrap();
        assert_eq!(again, Appended { offset: first.offset, ..Appended::default() });

        let line = serde_json::to_vec(&records[2]).unwrap();
        let (head, tail) = line.split_at(line.len() / 2);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(head).unwrap();
        let torn = read_appended(&path, first.offset).unwrap();
        assert_eq!(torn, Appended { offset: first.offset, ..Appended::default() });

        file.write_all(tail).unwrap();
        file.write_all(b"\nnot json\n").unwrap();
        writer.append(&records[3]).unwrap();
        let rest = read_appended(&path, torn.offset).unwrap();
        assert_eq!(rest.records, [records[2].clone(), records[3].clone()]);
        assert_eq!(rest.malformed_lines, 1);
        assert_eq!(rest.offset, fs::metadata(&path).unwrap().len());
    }

    #[test]
    fn landed_and_refused_outcomes() {
        let snapshot = Some("SNAP".to_owned());
        let message = || "m".to_owned();
        assert_eq!(
            OpResult::Ack { snapshot: snapshot.clone() }.landed(),
            Some(Some("SNAP"))
        );
        assert_eq!(OpResult::Ack { snapshot: None }.landed(), Some(None));
        let durable = OpResult::DurableButForked { snapshot, message: message() };
        assert_eq!(durable.landed(), Some(Some("SNAP")));
        assert!(!durable.is_refusal());
        for refused in [
            OpResult::ReadOnly { message: message() },
            OpResult::Forked { message: message() },
            OpResult::ConversionInProgress { message: message() },
        ] {
            assert!(refused.is_refusal(), "{refused:?}");
            assert_eq!(refused.landed(), None, "{refused:?}");
        }
        for failed in [OpResult::Conflict, OpResult::Error { message: message() }] {
            assert!(!failed.is_refusal() && failed.landed().is_none(), "{failed:?}");
        }
    }

    #[test]
    fn log_names_parse() {
        let worker = |tag: &str, slot: &str, n| LogName {
            tag: tag.to_owned(),
            slot: slot.to_owned(),
            incarnation: Some(n),
        };
        let supervisor = |tag: &str| LogName {
            tag: tag.to_owned(),
            slot: SUPERVISOR_SLOT.to_owned(),
            incarnation: None,
        };
        assert_eq!(LogName::parse("h-1.3.0.log"), Some(worker("h-1", "3", 0)));
        assert_eq!(
            LogName::parse("a.b_c-9.gc.12.log"),
            Some(worker("a.b_c-9", "gc", 12))
        );
        assert_eq!(LogName::parse("h-1.supervisor.log"), Some(supervisor("h-1")));
        assert_eq!(LogName::parse("a.5.supervisor.log"), Some(supervisor("a.5")));
        let sampler = |tag: &str| LogName {
            tag: tag.to_owned(),
            slot: SAMPLER_SLOT.to_owned(),
            incarnation: None,
        };
        assert_eq!(LogName::parse("h-1.sampler.log"), Some(sampler("h-1")));
        assert_eq!(LogName::parse("a.5.sampler.log"), Some(sampler("a.5")));
        for bad in [
            "h.3.0",
            "h.3.log",
            ".3.0.log",
            "h..0.log",
            "h.3.x.log",
            "h.3.-1.log",
            "h.3.01.log",
            "h.3.+1.log",
            "h.supervisor.0.log",
            "h.sampler.0.log",
            "sampler.log",
            "h space.3.0.log",
            "supervisor.log",
            "h.3.0.log.tmp",
            "notes.txt",
        ] {
            assert_eq!(LogName::parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn read_all_logs_parses_names_and_rejects_others() {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().to_path_buf());
        fs::create_dir_all(root.logs_dir()).unwrap();
        let start = Record::Start {
            t_ms: 1,
            host: "h.x".to_owned(),
            slot: "gc".to_owned(),
            incarnation: 2,
            pid: 9,
        };
        let spawn =
            Record::Spawn { t_ms: 1, slot: "gc".to_owned(), incarnation: 2, pid: 9 };
        LogWriter::create(&root.worker_log("h.x", "gc", 2))
            .unwrap()
            .append(&start)
            .unwrap();
        LogWriter::create(&root.supervisor_log("h.x")).unwrap().append(&spawn).unwrap();

        let logs = read_all_logs(&root).unwrap();
        let expected = BTreeMap::from([
            (
                LogName {
                    tag: "h.x".to_owned(),
                    slot: "gc".to_owned(),
                    incarnation: Some(2),
                },
                vec![start],
            ),
            (
                LogName {
                    tag: "h.x".to_owned(),
                    slot: SUPERVISOR_SLOT.to_owned(),
                    incarnation: None,
                },
                vec![spawn],
            ),
        ]);
        assert_eq!(logs, expected);

        fs::write(root.logs_dir().join("stray.txt"), b"").unwrap();
        let err = format!("{:#}", read_all_logs(&root).unwrap_err());
        assert!(err.contains("stray.txt"), "{err}");
    }
}
