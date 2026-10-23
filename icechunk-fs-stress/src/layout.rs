//! Paths under the run root.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use anyhow::{Context as _, ensure};

use crate::config::Mode;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRoot {
    root: PathBuf,
    /// The storage root when it isn't `<root>/storage`: a phase of the
    /// conversion scenario shares the scenario's.
    storage: Option<PathBuf>,
}

impl RunRoot {
    pub fn new(root: PathBuf) -> Self {
        Self { root, storage: None }
    }

    /// A run root whose storage root is `storage`.
    pub fn with_storage(root: PathBuf, storage: PathBuf) -> Self {
        Self { root, storage: Some(storage) }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Creates the root, which must be missing or empty, and its directories.
    pub fn create_dirs(&self) -> anyhow::Result<()> {
        create_empty_dir(&self.root)?;
        [self.hosts_dir(), self.done_dir(), self.logs_dir(), self.storage()]
            .iter()
            .try_for_each(|dir| {
                fs::create_dir_all(dir)
                    .with_context(|| format!("creating {}", dir.display()))
            })
    }

    pub fn run_json(&self) -> PathBuf {
        self.root.join("run.json")
    }

    pub fn control(&self) -> PathBuf {
        self.root.join("control")
    }

    /// The participant list, written once at the go.
    pub fn go(&self) -> PathBuf {
        self.root.join("go")
    }

    pub fn hosts_dir(&self) -> PathBuf {
        self.root.join("hosts")
    }

    pub fn host_info(&self, tag: &str) -> PathBuf {
        self.hosts_dir().join(format!("{tag}.json"))
    }

    pub fn done_dir(&self) -> PathBuf {
        self.root.join("done")
    }

    pub fn host_done(&self, tag: &str) -> PathBuf {
        self.done_dir().join(format!("{tag}.json"))
    }

    pub fn logs_dir(&self) -> PathBuf {
        self.root.join("logs")
    }

    pub fn worker_log(&self, tag: &str, slot: &str, incarnation: u32) -> PathBuf {
        self.logs_dir().join(format!("{tag}.{slot}.{incarnation}.log"))
    }

    pub fn supervisor_log(&self, tag: &str) -> PathBuf {
        self.logs_dir().join(format!("{tag}.{SUPERVISOR_SLOT}.log"))
    }

    pub fn sampler_log(&self, tag: &str) -> PathBuf {
        self.logs_dir().join(format!("{tag}.{SAMPLER_SLOT}.log"))
    }

    /// `qualify`'s detailed progress of the host `tag`, or of verify.
    pub fn progress(&self, tag: &str) -> PathBuf {
        self.root.join("progress").join(format!("{tag}.txt"))
    }

    pub fn storage(&self) -> PathBuf {
        self.storage.clone().unwrap_or_else(|| self.root.join("storage"))
    }

    /// The storage root, if it isn't `<root>/storage`.
    pub fn storage_override(&self) -> Option<&Path> {
        self.storage.as_deref()
    }

    pub fn report_json(&self) -> PathBuf {
        self.root.join("report.json")
    }

    /// The summary printed with the report, next to `report.json`.
    pub fn summary(&self) -> PathBuf {
        self.root.join("summary.txt")
    }
}

/// Creates `dir`, which must be missing or empty.
fn create_empty_dir(dir: &Path) -> anyhow::Result<()> {
    match fs::read_dir(dir) {
        Ok(mut entries) => {
            ensure!(entries.next().is_none(), "{} is not empty", dir.display());
            Ok(())
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))
        }
        Err(err) => Err(err).with_context(|| format!("reading {}", dir.display())),
    }
}

/// The root of `qualify`: `qualify.json`, a run root for each mode,
/// `storage-mode/` and `repository-mode/`, and `summary.txt`, the combined
/// summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifyRoot {
    root: PathBuf,
}

impl QualifyRoot {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Creates the root, which must be missing or empty.
    pub fn create_dir(&self) -> anyhow::Result<()> {
        create_empty_dir(&self.root)
    }

    pub fn qualify_json(&self) -> PathBuf {
        self.root.join("qualify.json")
    }

    /// The run root of `mode`'s run.
    pub fn run(&self, mode: Mode) -> RunRoot {
        RunRoot::new(self.root.join(qualify_run_dir(mode)))
    }

    pub fn summary(&self) -> PathBuf {
        self.root.join("summary.txt")
    }
}

/// The directory of `mode`'s run under a [`QualifyRoot`].
pub fn qualify_run_dir(mode: Mode) -> &'static str {
    match mode {
        Mode::Storage => "storage-mode",
        Mode::Repository => "repository-mode",
    }
}

/// The conversion scenario's root: one `storage/` that every phase shares,
/// `scenario.json`, `report.json`, `summary.txt`, and a run root `phase-<k>/`
/// for each round `k`, from 1, holding that round's logs, its stress phase's
/// `run.json`, `control`, `go`, `hosts/`, `done/`, `report.json` and
/// `summary.txt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioRoot {
    root: PathBuf,
}

impl ScenarioRoot {
    /// Made absolute: its phases' `run.json` name its storage root.
    pub fn new(root: PathBuf) -> Self {
        Self { root: std::path::absolute(&root).unwrap_or(root) }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn storage(&self) -> PathBuf {
        self.root.join("storage")
    }

    pub fn scenario_json(&self) -> PathBuf {
        self.root.join("scenario.json")
    }

    /// The scenario's own run root, for its `report.json` and `summary.txt`.
    pub fn top(&self) -> RunRoot {
        RunRoot::with_storage(self.root.clone(), self.storage())
    }

    pub fn phase(&self, round: u32) -> RunRoot {
        RunRoot::with_storage(self.root.join(format!("phase-{round}")), self.storage())
    }
}

/// The slot name of a supervisor log; no worker slot may use it.
pub const SUPERVISOR_SLOT: &str = "supervisor";

/// The slot of a supervisor's mirror sampler log; no worker slot may use it.
pub const SAMPLER_SLOT: &str = "sampler";

/// Reserved for `verify`'s own writes; no supervisor may use it.
pub const VERIFY_TAG: &str = "verify";

/// Reserved for `init`'s log, `logs/init.init.0.log`; no supervisor may use
/// it.
pub const INIT_TAG: &str = "init";

/// Reserved for the conversion scenario's own logs in a phase: its
/// converters, its writers in the standard layout and during an interrupted
/// conversion, and its planted standard write.
pub const SCENARIO_TAG: &str = "scenario";

/// Host tags are `[A-Za-z0-9._-]+`.
pub fn is_valid_tag(tag: &str) -> bool {
    !tag.is_empty() && tag.chars().all(is_tag_char)
}

/// Slots are tag characters without `.`, and never [`SUPERVISOR_SLOT`] or
/// [`SAMPLER_SLOT`].
pub fn is_valid_slot(slot: &str) -> bool {
    !slot.is_empty()
        && slot != SUPERVISOR_SLOT
        && slot != SAMPLER_SLOT
        && slot.chars().all(|c| c != '.' && is_tag_char(c))
}

pub(crate) fn is_tag_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}
