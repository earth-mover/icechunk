//! The `control` file, through which `init`, the barrier and `stop` drive
//! every host. It is only ever replaced by a rename, so readers see a whole
//! state.
//!
//! The participant list is decided once, by whichever host first hard-links
//! its list to `go`; that host writes the go to `control`. If `control` still
//! says wait well after `go` appeared, any waiting host copies `go` there.
//! `go` outlives a later `stop`, so it is the authoritative participant list.

use std::{
    collections::BTreeSet,
    fs, io,
    io::Write as _,
    os::unix::fs::MetadataExt as _,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context as _, bail};

use crate::{
    config::write_synced,
    env_info::{HostInfo, host_info},
    layout::{RunRoot, is_valid_tag},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    Wait,
    Go { participants: BTreeSet<String> },
    Stop,
}

impl Control {
    fn encode(&self) -> io::Result<String> {
        match self {
            Self::Wait => Ok("wait\n".to_owned()),
            Self::Stop => Ok("stop\n".to_owned()),
            Self::Go { participants } => Ok(format!(
                "go\n{}",
                encode_participants(participants).map_err(io::Error::other)?
            )),
        }
    }

    fn decode(content: &str) -> anyhow::Result<Self> {
        let Some(body) = content.strip_suffix('\n') else {
            bail!("control doesn't end with a newline: {content:?}");
        };
        let mut lines = body.split('\n');
        match lines.next() {
            Some("wait") if body == "wait" => Ok(Self::Wait),
            Some("stop") if body == "stop" => Ok(Self::Stop),
            Some("go") => Ok(Self::Go { participants: decode_participants(lines)? }),
            _ => bail!("invalid control: {content:?}"),
        }
    }
}

/// One tag per line, each followed by a newline.
fn encode_participants(participants: &BTreeSet<String>) -> anyhow::Result<String> {
    check_participants(participants)?;
    Ok(participants.iter().fold(String::new(), |mut s, tag| {
        s.push_str(tag);
        s.push('\n');
        s
    }))
}

fn decode_participants<'a>(
    lines: impl Iterator<Item = &'a str>,
) -> anyhow::Result<BTreeSet<String>> {
    let tags: Vec<&str> = lines.collect();
    let participants: BTreeSet<String> = tags.iter().map(|&tag| tag.to_owned()).collect();
    if participants.len() != tags.len() {
        bail!("a participant is listed twice");
    }
    check_participants(&participants)?;
    Ok(participants)
}

fn check_participants(participants: &BTreeSet<String>) -> anyhow::Result<()> {
    if participants.is_empty() {
        bail!("go lists no participants");
    }
    if let Some(bad) = participants.iter().find(|tag| !is_valid_tag(tag)) {
        bail!("invalid participant tag {bad:?}");
    }
    Ok(())
}

pub fn write_control(root: &RunRoot, state: &Control) -> io::Result<()> {
    let content = state.encode()?;
    let staging = root.root().join(format!("control.{}.tmp", uuid::Uuid::new_v4()));
    let written = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)
        .and_then(|mut file| {
            file.write_all(content.as_bytes())?;
            file.sync_all()
        })
        .and_then(|()| fs::rename(&staging, root.control()));
    if written.is_err() {
        // Best effort: the error being reported is the write's.
        let _ = fs::remove_file(&staging);
    }
    written
}

/// Reads `control` afresh: never cached, so every call sees the latest rename.
pub fn read_control(root: &RunRoot) -> anyhow::Result<Control> {
    let path = root.control();
    let content = fs::read_to_string(&path)
        .with_context(|| format!("reading {}", path.display()))?;
    Control::decode(&content).with_context(|| format!("parsing {}", path.display()))
}

/// Whether a worker should stop: at `until`, or once `control` reads `stop`,
/// which is checked at most every [`Self::CONTROL_EVERY`]. A missing or
/// unreadable `control` doesn't stop the worker.
#[derive(Debug)]
pub struct StopCheck<'a> {
    root: &'a RunRoot,
    until: SystemTime,
    next_control_check: Instant,
}

impl<'a> StopCheck<'a> {
    const CONTROL_EVERY: Duration = Duration::from_secs(1);

    pub fn new(root: &'a RunRoot, until: SystemTime) -> Self {
        Self { root, until, next_control_check: Instant::now() }
    }

    pub fn stopped(&mut self) -> bool {
        if SystemTime::now() >= self.until {
            return true;
        }
        let now = Instant::now();
        if now < self.next_control_check {
            return false;
        }
        self.next_control_check = now + Self::CONTROL_EVERY;
        matches!(read_control(self.root), Ok(Control::Stop))
    }
}

/// Writes `hosts/<tag>.json`; an error if the tag is already registered.
pub fn register(root: &RunRoot, info: &HostInfo) -> anyhow::Result<()> {
    let path = root.host_info(&info.tag);
    let bytes = serde_json::to_vec_pretty(info)?;
    let written = write_synced(&path, &bytes);
    if let Err(err) = &written
        && err.kind() != io::ErrorKind::AlreadyExists
    {
        // Best effort: a partial file would still count as a registration.
        let _ = fs::remove_file(&path);
    }
    written.with_context(|| format!("registering {}", path.display()))
}

/// The tags registered under `hosts/`, which must contain nothing else.
pub fn registered(root: &RunRoot) -> anyhow::Result<BTreeSet<String>> {
    let dir = root.hosts_dir();
    let context = || format!("listing {}", dir.display());
    fs::read_dir(&dir)
        .with_context(context)?
        .map(|entry| {
            let path = entry.with_context(context)?.path();
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(".json"))
                .filter(|tag| is_valid_tag(tag))
                .map(str::to_owned)
                .with_context(|| format!("unexpected file {}", path.display()))
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitOutcome {
    /// `participants` is the go list as this host read it.
    Participant { go_seen_at: SystemTime, participants: BTreeSet<String> },
    /// The run started or stopped without this host; it is not registered.
    Late,
}

pub const BARRIER_POLL: Duration = Duration::from_millis(100);

/// How long a host waits with `go` decided but `control` still `wait` before
/// it writes the go itself: the decider died between the two, or misread its
/// own link as lost.
const GO_GRACE: Duration = Duration::from_secs(5);

/// Consecutive failed polls after registering before a host gives up.
const BARRIER_ATTEMPTS: u32 = 5;

/// The barrier: registers `tag` and waits until `control` says go. A host that
/// sees `expected` registrations tries to decide the go list (see
/// [`try_go`]); every host takes its list from `control`.
pub fn wait_for_go(
    root: &RunRoot,
    tag: &str,
    expected: u32,
    poll: Duration,
) -> anyhow::Result<WaitOutcome> {
    wait_for_go_reporting(root, tag, expected, poll, Duration::ZERO)
}

/// [`wait_for_go`], printing how many hosts registered to stderr every
/// `report_every` (never if zero). Once registered, a poll that fails is
/// retried; after [`BARRIER_ATTEMPTS`] failures in a row the host unregisters
/// and gives up.
pub fn wait_for_go_reporting(
    root: &RunRoot,
    tag: &str,
    expected: u32,
    poll: Duration,
    report_every: Duration,
) -> anyhow::Result<WaitOutcome> {
    let expected = usize::try_from(expected)?;
    if read_control(root)? != Control::Wait {
        return Ok(WaitOutcome::Late);
    }
    register(root, &host_info(tag, &root.storage()))?;
    let mut lost_go = false;
    let mut go_seen_at: Option<Instant> = None;
    let mut next_report = Instant::now();
    let mut step = || -> anyhow::Result<Option<WaitOutcome>> {
        match read_control(root)? {
            Control::Go { participants } if participants.contains(tag) => {
                return Ok(Some(WaitOutcome::Participant {
                    go_seen_at: SystemTime::now(),
                    participants,
                }));
            }
            Control::Go { .. } | Control::Stop => {
                unregister(root, tag)?;
                return Ok(Some(WaitOutcome::Late));
            }
            Control::Wait => {}
        }
        let participants = registered(root)?;
        let now = Instant::now();
        if !report_every.is_zero() && now >= next_report {
            eprintln!(
                "registered as {tag}; waiting for go ({}/{expected} hosts registered)",
                participants.len()
            );
            next_report = now + report_every;
        }
        if !lost_go && participants.len() >= expected {
            if try_go(root, &participants)? {
                return Ok(None);
            }
            lost_go = true;
        }
        // Identical content, so it doesn't matter how many hosts write it.
        if let Ok(go) = read_go(root)
            && go_seen_at.get_or_insert(now).elapsed() > GO_GRACE
        {
            write_control(root, &Control::Go { participants: go })
                .context("copying go to control")?;
        }
        Ok(None)
    };
    let mut failures = 0;
    loop {
        match step() {
            Ok(Some(outcome)) => return Ok(outcome),
            Ok(None) => failures = 0,
            Err(err) => {
                failures += 1;
                if failures >= BARRIER_ATTEMPTS {
                    // Best effort: the error being reported is the barrier's.
                    let _ = unregister(root, tag);
                    return Err(err.context(format!(
                        "waiting for go, {failures} attempts in a row"
                    )));
                }
                tracing::warn!("waiting for go: {err:#}; retrying");
            }
        }
        std::thread::sleep(poll);
    }
}

/// Removes `hosts/<tag>.json`; already gone is fine, for a retried removal.
fn unregister(root: &RunRoot, tag: &str) -> anyhow::Result<()> {
    let path = root.host_info(tag);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("unregistering {}", path.display())),
    }
}

/// Decides the go list unless another host already did: hard-links a staged
/// copy of `participants` to `go`, and only if that link is the first writes
/// the go to `control`. Returns whether this call decided.
fn try_go(root: &RunRoot, participants: &BTreeSet<String>) -> anyhow::Result<bool> {
    let go = root.go();
    let staging = root.root().join(format!("go.{}.tmp", uuid::Uuid::new_v4()));
    let decided = write_synced(&staging, encode_participants(participants)?.as_bytes())
        .and_then(|()| match fs::hard_link(&staging, &go) {
            Ok(()) => Ok(true),
            // On NFS a retransmitted LINK can report EEXIST for our own
            // link; the staged file's link count tells.
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                Ok(fs::metadata(&staging)?.nlink() == 2)
            }
            Err(err) => Err(err),
        })
        .with_context(|| format!("writing {}", go.display()))
        .and_then(|decided| {
            if decided {
                let participants = participants.clone();
                write_control(root, &Control::Go { participants })
                    .context("writing the go to control")?;
            }
            Ok(decided)
        });
    // Best effort: `go` holds the content, and an error being reported wins.
    let _ = fs::remove_file(&staging);
    decided
}

/// The participant list decided at the go.
pub fn read_go(root: &RunRoot) -> anyhow::Result<BTreeSet<String>> {
    let path = root.go();
    let content = fs::read_to_string(&path)
        .with_context(|| format!("reading {}", path.display()))?;
    content
        .strip_suffix('\n')
        .context("doesn't end with a newline")
        .and_then(|body| decode_participants(body.split('\n')))
        .with_context(|| format!("parsing {}", path.display()))
}

/// Tells every host to stop; `root` must be a run root.
pub fn stop(root: &RunRoot) -> anyhow::Result<()> {
    read_control(root)?;
    write_control(root, &Control::Stop).context("writing stop")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_root() -> (tempfile::TempDir, RunRoot) {
        let dir = tempfile::tempdir().unwrap();
        let root = RunRoot::new(dir.path().to_path_buf());
        (dir, root)
    }

    #[test]
    fn states_round_trip_and_leave_no_staging() {
        let (dir, root) = run_root();
        let go = Control::Go {
            participants: BTreeSet::from(["h-2".to_owned(), "a.b_c".to_owned()]),
        };
        for state in [Control::Wait, go, Control::Stop] {
            write_control(&root, &state).unwrap();
            assert_eq!(read_control(&root).unwrap(), state);
        }
        assert_eq!(fs::read_to_string(root.control()).unwrap(), "stop\n");
        let names: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, ["control"]);
    }

    #[test]
    fn go_lists_one_tag_per_line() {
        let (_dir, root) = run_root();
        let participants = BTreeSet::from(["b".to_owned(), "a".to_owned()]);
        write_control(&root, &Control::Go { participants }).unwrap();
        assert_eq!(fs::read_to_string(root.control()).unwrap(), "go\na\nb\n");
    }

    #[test]
    fn garbage_is_an_error() {
        let (_dir, root) = run_root();
        assert!(read_control(&root).is_err(), "missing control");
        for bad in [
            "",
            "\n",
            "wait",
            "stop",
            "go",
            "go\n",
            "Wait\n",
            "wait\n\n",
            "wait\nh\n",
            "stop\nh\n",
            "go\nh\n\n",
            "go\nh\nh\n",
            "go\nh space\n",
            "go\nh",
            "start\n",
        ] {
            fs::write(root.control(), bad).unwrap();
            assert!(read_control(&root).is_err(), "{bad:?}");
        }
    }

    fn info(tag: &str, root: &RunRoot) -> HostInfo {
        host_info(tag, root.root())
    }

    const POLL: Duration = Duration::from_millis(5);

    fn tags<const N: usize>(tags: [&str; N]) -> BTreeSet<String> {
        tags.into_iter().map(str::to_owned).collect()
    }

    fn barrier_root() -> (tempfile::TempDir, RunRoot) {
        let (dir, root) = run_root();
        fs::create_dir_all(root.hosts_dir()).unwrap();
        write_control(&root, &Control::Wait).unwrap();
        (dir, root)
    }

    #[test]
    fn racing_flippers_decide_one_go_list_that_every_host_follows() {
        let all = tags(["a", "b", "c", "d"]);
        for _ in 0..20 {
            let (dir, root) = barrier_root();
            let root = &root;
            let go = std::thread::scope(|s| {
                // These hosts never flip themselves.
                let waiters: Vec<_> = all
                    .iter()
                    .map(|tag| (tag, s.spawn(move || wait_for_go(root, tag, 100, POLL))))
                    .collect();
                while registered(root).unwrap() != all {
                    std::thread::sleep(POLL);
                }
                let flips: Vec<_> = [tags(["a", "b"]), all.clone()]
                    .into_iter()
                    .map(|list| s.spawn(move || (try_go(root, &list).unwrap(), list)))
                    .collect();
                let flips: Vec<_> =
                    flips.into_iter().map(|f| f.join().unwrap()).collect();
                let winners: Vec<_> = flips.iter().filter(|(won, _)| *won).collect();
                assert_eq!(winners.len(), 1, "{flips:?}");
                let go = read_go(root).unwrap();
                assert_eq!(winners[0].1, go);
                assert_eq!(
                    read_control(root).unwrap(),
                    Control::Go { participants: go.clone() }
                );
                for (tag, waiter) in waiters {
                    match waiter.join().unwrap().unwrap() {
                        WaitOutcome::Participant { participants, .. } => {
                            assert!(go.contains(tag), "{tag}");
                            assert_eq!(participants, go);
                        }
                        WaitOutcome::Late => {
                            assert!(!go.contains(tag), "{tag}");
                            assert!(!root.host_info(tag).exists(), "{tag}");
                        }
                    }
                }
                go
            });

            // A loser never writes control.
            write_control(root, &Control::Stop).unwrap();
            assert!(!try_go(root, &tags(["a"])).unwrap());
            assert_eq!(read_control(root).unwrap(), Control::Stop);
            assert_eq!(read_go(root).unwrap(), go);
            let staged: Vec<_> = fs::read_dir(dir.path())
                .unwrap()
                .map(|e| e.unwrap().file_name().into_string().unwrap())
                .filter(|name| name.ends_with(".tmp"))
                .collect();
            assert!(staged.is_empty(), "{staged:?}");
        }
    }

    #[test]
    fn a_go_never_written_to_control_is_copied_there_after_the_grace_period() {
        let (_dir, root) = barrier_root();
        let go = tags(["a", "b"]);
        // As a decider leaves it if it dies between the link and the control
        // write, or misreads its own link as lost.
        fs::write(root.go(), encode_participants(&go).unwrap()).unwrap();
        let started = Instant::now();
        let root = &root;
        let outcomes: Vec<_> = std::thread::scope(|s| {
            let waiters: Vec<_> = ["a", "b", "c"]
                .into_iter()
                .map(|tag| (tag, s.spawn(move || wait_for_go(root, tag, 100, POLL))))
                .collect();
            waiters
                .into_iter()
                .map(|(tag, w)| (tag, w.join().unwrap().unwrap()))
                .collect()
        });
        assert!(started.elapsed() >= GO_GRACE, "{:?}", started.elapsed());
        assert_eq!(read_control(root).unwrap(), Control::Go { participants: go.clone() });
        assert_eq!(read_go(root).unwrap(), go);
        for (tag, outcome) in outcomes {
            match outcome {
                WaitOutcome::Participant { participants, .. } => {
                    assert!(go.contains(tag), "{tag}");
                    assert_eq!(participants, go);
                }
                WaitOutcome::Late => {
                    assert!(!go.contains(tag), "{tag}");
                    assert!(!root.host_info(tag).exists(), "{tag}");
                }
            }
        }
    }

    #[test]
    fn a_barrier_that_keeps_failing_unregisters() {
        let (_dir, root) = barrier_root();
        let bad = root.hosts_dir().join("bad tag.json");
        fs::write(&bad, "{}").unwrap();
        let err = wait_for_go(&root, "a", 1, POLL).unwrap_err();
        assert!(format!("{err:#}").contains("bad tag.json"), "{err:#}");
        assert!(!root.host_info("a").exists());
        assert!(bad.exists());
        assert_eq!(read_control(&root).unwrap(), Control::Wait);
    }

    #[test]
    fn go_round_trips_and_garbage_is_an_error() {
        let (_dir, root) = barrier_root();
        assert!(read_go(&root).is_err(), "missing go");
        assert!(try_go(&root, &tags(["b", "a"])).unwrap());
        assert_eq!(fs::read_to_string(root.go()).unwrap(), "a\nb\n");
        assert_eq!(read_go(&root).unwrap(), tags(["a", "b"]));
        for bad in ["", "\n", "a", "a\na\n", "a b\n", "a\n\n", "go\na\n\n"] {
            fs::write(root.go(), bad).unwrap();
            assert!(read_go(&root).is_err(), "{bad:?}");
        }
        assert!(try_go(&root, &BTreeSet::new()).is_err());
    }

    #[test]
    fn stop_needs_a_run_root() {
        let (_dir, root) = run_root();
        assert!(stop(&root).is_err());
        assert!(!root.control().exists());
        write_control(&root, &Control::Wait).unwrap();
        stop(&root).unwrap();
        assert_eq!(read_control(&root).unwrap(), Control::Stop);
    }

    #[test]
    fn register_writes_host_info_once() {
        let (_dir, root) = run_root();
        fs::create_dir_all(root.hosts_dir()).unwrap();
        assert_eq!(registered(&root).unwrap(), BTreeSet::new());

        let a = info("a", &root);
        register(&root, &a).unwrap();
        register(&root, &info("b.c", &root)).unwrap();
        assert!(register(&root, &info("a", &root)).is_err());
        let written: HostInfo =
            serde_json::from_slice(&fs::read(root.host_info("a")).unwrap()).unwrap();
        assert_eq!(written, a);
        assert_eq!(
            registered(&root).unwrap(),
            BTreeSet::from(["a".to_owned(), "b.c".to_owned()])
        );

        fs::write(root.hosts_dir().join("bad tag.json"), "{}").unwrap();
        assert!(registered(&root).is_err());
    }

    #[test]
    fn invalid_go_is_not_written() {
        let (_dir, root) = run_root();
        for participants in [BTreeSet::new(), BTreeSet::from(["a/b".to_owned()])] {
            assert!(write_control(&root, &Control::Go { participants }).is_err());
        }
        assert!(!root.control().exists());
    }
}
