//! Paths under the run root.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use anyhow::{Context as _, ensure};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRoot {
    root: PathBuf,
}

impl RunRoot {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Creates the root, which must be missing or empty, and its directories.
    pub fn create_dirs(&self) -> anyhow::Result<()> {
        let root = &self.root;
        match fs::read_dir(root) {
            Ok(mut entries) => {
                ensure!(entries.next().is_none(), "{} is not empty", root.display());
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(root)
                    .with_context(|| format!("creating {}", root.display()))?;
            }
            Err(err) => {
                return Err(err).with_context(|| format!("reading {}", root.display()));
            }
        }
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

    pub fn storage(&self) -> PathBuf {
        self.root.join("storage")
    }

    pub fn report_json(&self) -> PathBuf {
        self.root.join("report.json")
    }
}

/// The slot name of a supervisor log; no worker slot may use it.
pub const SUPERVISOR_SLOT: &str = "supervisor";

/// Reserved for `verify`'s own writes; no supervisor may use it.
pub const VERIFY_TAG: &str = "verify";

/// Host tags are `[A-Za-z0-9._-]+`.
pub fn is_valid_tag(tag: &str) -> bool {
    !tag.is_empty() && tag.chars().all(is_tag_char)
}

/// Slots are tag characters without `.`, and never [`SUPERVISOR_SLOT`].
pub fn is_valid_slot(slot: &str) -> bool {
    !slot.is_empty()
        && slot != SUPERVISOR_SLOT
        && slot.chars().all(|c| c != '.' && is_tag_char(c))
}

pub(crate) fn is_tag_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')
}
