//! `bundle`: a run's results in one `.tar.gz` file to send us.
//!
//! Every regular file under the root goes in, apart from the storage roots,
//! directories named `storage`. Unless every report passed, each storage
//! root's `repo`, `repo.d/` and a listing of its files go in too.

use std::{
    collections::BTreeSet,
    ffi::OsString,
    fmt::Write as _,
    fs::{self, File, Metadata, OpenOptions},
    io::{self, BufWriter, Read, Write as _},
    os::unix::fs::{FileTypeExt as _, MetadataExt as _, OpenOptionsExt as _},
    path::{Component, Path, PathBuf},
};

use anyhow::{Context as _, ensure};
use chrono::{DateTime, SecondsFormat, Utc};
use flate2::{Compression, write::GzEncoder};
use serde::{Deserialize, Serialize};
use tar::{EntryType, Header};

use crate::{config::Mode, env_info::Build, layout::QualifyRoot};

/// At the top of the bundle.
pub const BUNDLE_JSON: &str = "bundle.json";

/// Next to each storage root in a failed run's bundle.
pub const STORAGE_LISTING: &str = "storage-listing.txt";

const STORAGE: &str = "storage";
const REPORT: &str = "report.json";

/// One of these in the root makes it a run root, a qualify root or a
/// conversion scenario root.
const ROOT_MARKERS: [&str; 3] = ["run.json", "qualify.json", "scenario.json"];

/// `bundle.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleInfo {
    /// The binary that made the bundle.
    pub build: Build,
    pub created_at: DateTime<Utc>,
    pub command_line: Vec<String>,
    /// Whether every report under the root passed.
    pub passed: bool,
    /// The storage roots, relative to the root, whose `repo`, `repo.d/` and
    /// listing are in the bundle.
    pub repo_info_from: Vec<String>,
    /// Files left out, or cut short because reading them failed.
    pub skipped: Vec<Skipped>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Skipped {
    /// Relative to the root.
    pub path: String,
    pub reason: String,
}

/// What a bundle of a root holds, found before any file is read.
#[derive(Debug)]
pub struct Bundle {
    root: PathBuf,
    root_meta: Metadata,
    passed: bool,
    found: Found,
}

/// The written bundle.
#[derive(Debug)]
pub struct Bundled {
    pub path: PathBuf,
    pub size: u64,
    /// Files left out or cut short, listed in `bundle.json`.
    pub skipped: usize,
}

impl Bundle {
    /// Walks `root`, which must be a run root, a qualify root or a
    /// conversion scenario root, and reads its reports.
    pub fn collect(root: &Path) -> anyhow::Result<Self> {
        let root_meta =
            fs::metadata(root).with_context(|| format!("reading {}", root.display()))?;
        // Before any walk: a mistyped root may be a whole shared filesystem.
        ensure!(
            ROOT_MARKERS.iter().any(|marker| {
                fs::symlink_metadata(root.join(marker)).is_ok_and(|meta| meta.is_file())
            }),
            "{} is not a run root, a qualify root or a conversion scenario root: \
             it has no {}",
            root.display(),
            ROOT_MARKERS.join(", ")
        );
        let mut found = Found::default();
        for name in read_dir_sorted(root)
            .with_context(|| format!("reading {}", root.display()))?
        {
            let rel = PathBuf::from(name);
            if is_earlier_bundle(&rel) {
                found.skip(&rel, "an earlier bundle".to_owned());
            } else {
                found.add(root, rel);
            }
        }
        let passed =
            found.reports().iter().all(|report| report_passed(&root.join(report)));
        Ok(Self { root: root.to_owned(), root_meta, passed, found })
    }

    /// Whether every report under the root passed, and every run root has
    /// one.
    pub fn passed(&self) -> bool {
        self.passed
    }

    /// Writes the bundle to `output`, which must not exist, or to
    /// [`default_output_name`] in the current directory. Files that vanished
    /// since [`Bundle::collect`] or fail to read are skipped and recorded.
    pub fn write(
        &self,
        output: Option<&Path>,
        command_line: &[String],
        now: DateTime<Utc>,
    ) -> anyhow::Result<Bundled> {
        let path = output.map_or_else(
            || PathBuf::from(default_output_name(self.passed, now)),
            Path::to_owned,
        );
        let top = top_dir(&path)?;
        let file = match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                anyhow::bail!("{} already exists; not overwriting it", path.display())
            }
            Err(err) => {
                return Err(err).with_context(|| format!("creating {}", path.display()));
            }
        };
        let written = self.write_to(file, &top, command_line, now);
        match written.and_then(|skipped| Ok((fs::metadata(&path)?.len(), skipped))) {
            Ok((size, skipped)) => Ok(Bundled { path, size, skipped }),
            Err(err) => {
                let _ = fs::remove_file(&path);
                Err(err.context(format!("writing {}", path.display())))
            }
        }
    }

    fn write_to(
        &self,
        file: File,
        top: &Path,
        command_line: &[String],
        now: DateTime<Utc>,
    ) -> anyhow::Result<usize> {
        let mut archive = Archive {
            builder: tar::Builder::new(GzEncoder::new(
                BufWriter::new(file),
                Compression::default(),
            )),
            root: &self.root,
            top,
            skipped: self.found.skipped.clone(),
        };
        archive.dir(Path::new(""), &self.root_meta)?;
        archive.entries(&self.found.entries)?;
        let repo_info_from =
            if self.passed { vec![] } else { self.found.storage.clone() };
        for storage in &repo_info_from {
            let mut found = Found::default();
            found.add_storage_root(&self.root, storage);
            archive.skipped.extend(found.skipped);
            archive.entries(&found.entries)?;
            let listing = storage_listing(&self.root.join(storage));
            let name = storage.parent().unwrap_or(Path::new("")).join(STORAGE_LISTING);
            archive.synthetic(&name, listing.as_bytes(), now)?;
        }
        let info = BundleInfo {
            build: Build::current(),
            created_at: now,
            command_line: command_line.to_vec(),
            passed: self.passed,
            repo_info_from: repo_info_from.iter().map(|p| lossy(p)).collect(),
            skipped: std::mem::take(&mut archive.skipped),
        };
        archive.synthetic(
            Path::new(BUNDLE_JSON),
            &serde_json::to_vec_pretty(&info)?,
            now,
        )?;
        let mut file = archive.builder.into_inner()?.finish()?;
        file.flush()?;
        file.into_inner().map_err(io::IntoInnerError::into_error)?.sync_all()?;
        Ok(info.skipped.len())
    }
}

/// `fs-stress-<pass|fail>-<UTC time>.tar.gz`.
pub fn default_output_name(passed: bool, now: DateTime<Utc>) -> String {
    let result = if passed { "pass" } else { "fail" };
    format!("fs-stress-{result}-{}.tar.gz", now.format("%Y%m%dT%H%M%SZ"))
}

/// The archive's top directory: the file's name without `.tar.gz` or `.tgz`.
fn top_dir(output: &Path) -> anyhow::Result<PathBuf> {
    let name = output
        .file_name()
        .with_context(|| format!("{} is not a file name", output.display()))?
        .to_string_lossy();
    let top = name
        .strip_suffix(".tar.gz")
        .or_else(|| name.strip_suffix(".tgz"))
        .unwrap_or(&name);
    let top = PathBuf::from(top);
    let mut components = top.components();
    ensure!(
        matches!(
            (components.next(), components.next()),
            (Some(Component::Normal(_)), None)
        ),
        "{}: the name before .tar.gz names the directory in the file, so it can't be \
         empty, . or ..",
        output.display()
    );
    Ok(top)
}

#[derive(Debug)]
enum Entry {
    Dir(PathBuf, Metadata),
    File(PathBuf),
}

/// Paths relative to the root.
#[derive(Debug, Default)]
struct Found {
    /// In walk order: a directory before its contents.
    entries: Vec<Entry>,
    storage: Vec<PathBuf>,
    skipped: Vec<Skipped>,
}

impl Found {
    fn add(&mut self, root: &Path, rel: PathBuf) {
        match fs::symlink_metadata(root.join(&rel)) {
            Err(err) => self.skip(&rel, err.to_string()),
            Ok(meta) if meta.is_dir() && rel.file_name() == Some(STORAGE.as_ref()) => {
                self.storage.push(rel);
            }
            Ok(meta) if meta.is_dir() => {
                self.entries.push(Entry::Dir(rel.clone(), meta));
                self.add_children(root, &rel);
            }
            Ok(meta) if meta.is_file() => self.entries.push(Entry::File(rel)),
            Ok(meta) => self.skip(&rel, not_regular(&meta).to_owned()),
        }
    }

    fn add_children(&mut self, root: &Path, rel: &Path) {
        match read_dir_sorted(&root.join(rel)) {
            Ok(names) => {
                names.into_iter().for_each(|name| self.add(root, rel.join(name)));
            }
            Err(err) => self.skip(rel, err.to_string()),
        }
    }

    /// The storage root `storage`'s `repo` and `repo.d/`.
    fn add_storage_root(&mut self, root: &Path, storage: &Path) {
        match fs::symlink_metadata(root.join(storage)) {
            Ok(meta) => self.entries.push(Entry::Dir(storage.to_owned(), meta)),
            Err(err) => self.skip(storage, err.to_string()),
        }
        self.add(root, storage.join("repo"));
        self.add(root, storage.join("repo.d"));
    }

    fn skip(&mut self, rel: &Path, reason: String) {
        self.skipped.push(Skipped { path: lossy(rel), reason });
    }

    /// Every `report.json` found, and those every run root, qualify root
    /// and scenario root should have.
    fn reports(&self) -> BTreeSet<PathBuf> {
        let files = self.entries.iter().filter_map(|entry| match entry {
            Entry::File(file) => Some(file),
            Entry::Dir(..) => None,
        });
        files
            .flat_map(|file| {
                let dir = file.parent().unwrap_or(Path::new(""));
                match file.file_name().and_then(|name| name.to_str()) {
                    Some(REPORT) => vec![file.clone()],
                    Some("run.json" | "scenario.json") => vec![dir.join(REPORT)],
                    Some("qualify.json") => {
                        let qualify = QualifyRoot::new(dir.to_owned());
                        [Mode::Storage, Mode::Repository]
                            .map(|mode| qualify.run(mode).report_json())
                            .to_vec()
                    }
                    _ => vec![],
                }
            })
            .collect()
    }
}

fn read_dir_sorted(dir: &Path) -> io::Result<Vec<OsString>> {
    let mut names = fs::read_dir(dir)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<io::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}

fn not_regular(meta: &Metadata) -> &'static str {
    let kind = meta.file_type();
    if kind.is_symlink() {
        "a symbolic link, not followed"
    } else if kind.is_socket() {
        "a socket"
    } else if kind.is_fifo() {
        "a FIFO"
    } else if kind.is_block_device() || kind.is_char_device() {
        "a device"
    } else {
        "not a regular file"
    }
}

/// One line per entry under `storage`, a directory before its contents:
/// its kind, size, modification time and path.
fn storage_listing(storage: &Path) -> String {
    let mut out = String::from(
        "# kind (d directory, f file, l symbolic link, o other), size in bytes, \
         modification time (UTC), path\n",
    );
    list(storage, Path::new(""), &mut out);
    out
}

fn list(storage: &Path, rel: &Path, out: &mut String) {
    let names = match read_dir_sorted(&storage.join(rel)) {
        Ok(names) => names,
        Err(err) => {
            let _ = writeln!(out, "? {}/: {err}", rel.display());
            return;
        }
    };
    for name in names {
        let rel = rel.join(name);
        let path = storage.join(&rel);
        let meta = match fs::symlink_metadata(&path) {
            Ok(meta) => meta,
            Err(err) => {
                let _ = writeln!(out, "? {}: {err}", rel.display());
                continue;
            }
        };
        let kind = meta.file_type();
        let mtime = u32::try_from(meta.mtime_nsec())
            .ok()
            .and_then(|nanos| DateTime::from_timestamp(meta.mtime(), nanos))
            .map_or_else(
                || "?".to_owned(),
                |t| t.to_rfc3339_opts(SecondsFormat::Nanos, true),
            );
        let (mark, suffix) = if kind.is_dir() {
            ("d", "/".to_owned())
        } else if kind.is_file() {
            ("f", String::new())
        } else if kind.is_symlink() {
            let target = fs::read_link(&path).map_or_else(
                |err| err.to_string(),
                |target| target.display().to_string(),
            );
            ("l", format!(" -> {target}"))
        } else {
            ("o", String::new())
        };
        let _ = writeln!(out, "{mark} {} {mtime} {}{suffix}", meta.len(), rel.display());
        if kind.is_dir() {
            list(storage, &rel, out);
        }
    }
}

/// A file `bundle` wrote with the root as the current directory.
fn is_earlier_bundle(name: &Path) -> bool {
    name.to_str().is_some_and(|name| {
        name.starts_with("fs-stress-")
            && (name.ends_with(".tar.gz") || name.ends_with(".tgz"))
    })
}

/// Never follows a symbolic link or blocks on a FIFO.
fn open_regular(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
}

/// Whether `path` is a regular file with a report that passed.
fn report_passed(path: &Path) -> bool {
    #[derive(Deserialize)]
    struct Passed {
        passed: bool,
    }
    let read = || -> anyhow::Result<bool> {
        let mut file = open_regular(path)?;
        ensure!(file.metadata()?.is_file(), "not a regular file");
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(serde_json::from_slice::<Passed>(&bytes)?.passed)
    };
    read().unwrap_or(false)
}

fn lossy(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

struct Archive<'a> {
    builder: tar::Builder<GzEncoder<BufWriter<File>>>,
    root: &'a Path,
    top: &'a Path,
    skipped: Vec<Skipped>,
}

impl Archive<'_> {
    fn entries(&mut self, entries: &[Entry]) -> io::Result<()> {
        entries.iter().try_for_each(|entry| match entry {
            Entry::Dir(rel, meta) => self.dir(rel, meta),
            Entry::File(rel) => self.file(rel),
        })
    }

    fn dir(&mut self, rel: &Path, meta: &Metadata) -> io::Result<()> {
        let mut header = Header::new_gnu();
        header.set_metadata(meta);
        header.set_size(0);
        self.builder.append_data(&mut header, self.top.join(rel), io::empty())
    }

    /// Errors are the archive's: the file's are recorded in `skipped`.
    fn file(&mut self, rel: &Path) -> io::Result<()> {
        let mut skip = |reason: String| {
            self.skipped.push(Skipped { path: lossy(rel), reason });
            Ok(())
        };
        let file = match open_regular(&self.root.join(rel)) {
            Ok(file) => file,
            Err(err) => return skip(err.to_string()),
        };
        let meta = match file.metadata() {
            Ok(meta) => meta,
            Err(err) => return skip(err.to_string()),
        };
        if !meta.is_file() {
            return skip(not_regular(&meta).to_owned());
        }
        let mut header = Header::new_gnu();
        header.set_metadata(&meta);
        let mut reader = ExactLen::new(file, meta.len());
        self.builder.append_data(&mut header, self.top.join(rel), &mut reader)?;
        if let Some(reason) = reader.cut_short() {
            self.skipped.push(Skipped { path: lossy(rel), reason });
        }
        Ok(())
    }

    /// A file the bundle makes.
    fn synthetic(
        &mut self,
        rel: &Path,
        contents: &[u8],
        now: DateTime<Utc>,
    ) -> io::Result<()> {
        let mut header = Header::new_gnu();
        header.set_entry_type(EntryType::Regular);
        header.set_mode(0o644);
        header.set_mtime(u64::try_from(now.timestamp()).unwrap_or(0));
        header.set_size(contents.len() as u64);
        self.builder.append_data(&mut header, self.top.join(rel), contents)
    }
}

/// Exactly `len` bytes of a file that may shrink or fail to read while it is
/// archived: zeros after its end or the error.
#[derive(Debug)]
struct ExactLen<R> {
    inner: R,
    len: u64,
    /// Bytes given so far.
    pos: u64,
    /// Bytes of them read from `inner`.
    read: u64,
    failed: Option<String>,
}

impl<R: Read> ExactLen<R> {
    fn new(inner: R, len: u64) -> Self {
        Self { inner, len, pos: 0, read: 0, failed: None }
    }

    /// Why the bytes after the first `read` are zeros, if they are.
    fn cut_short(&self) -> Option<String> {
        let failed = self.failed.as_ref()?;
        Some(format!(
            "{failed}: only the first {} of {} bytes are in the bundle",
            self.read, self.len
        ))
    }

    fn zeros(buf: &mut [u8]) -> usize {
        buf.fill(0);
        buf.len()
    }
}

impl<R: Read> Read for ExactLen<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let left = usize::try_from(self.len - self.pos).unwrap_or(usize::MAX);
        let len = left.min(buf.len());
        let buf = &mut buf[..len];
        if buf.is_empty() {
            return Ok(0);
        }
        let n = if self.failed.is_some() {
            Self::zeros(buf)
        } else {
            match self.inner.read(buf) {
                Ok(0) => {
                    self.failed = Some("it got shorter while it was read".to_owned());
                    Self::zeros(buf)
                }
                Ok(n) => {
                    self.read += n as u64;
                    n
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => return Err(err),
                Err(err) => {
                    self.failed = Some(err.to_string());
                    Self::zeros(buf)
                }
            }
        };
        self.pos += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Gives `data`, then fails.
    struct FailsAfter<'a>(&'a [u8]);

    impl Read for FailsAfter<'_> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.0.is_empty() {
                return Err(io::Error::other("disk on fire"));
            }
            let n = self.0.read(buf)?;
            Ok(n)
        }
    }

    fn read_all(reader: &mut impl Read) -> Vec<u8> {
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        out
    }

    #[test]
    fn a_file_is_archived_at_its_length_whatever_happens_while_it_is_read() {
        let mut whole = ExactLen::new(&b"abcdef"[..], 6);
        assert_eq!(read_all(&mut whole), b"abcdef");
        assert_eq!(whole.cut_short(), None);

        let mut grown = ExactLen::new(&b"abcdefgh"[..], 6);
        assert_eq!(read_all(&mut grown), b"abcdef");
        assert_eq!(grown.cut_short(), None);

        let mut shrunk = ExactLen::new(&b"abc"[..], 6);
        assert_eq!(read_all(&mut shrunk), b"abc\0\0\0");
        let reason = shrunk.cut_short().unwrap();
        assert!(reason.contains("only the first 3 of 6 bytes"), "{reason}");

        let mut failing = ExactLen::new(FailsAfter(b"ab"), 5);
        assert_eq!(read_all(&mut failing), b"ab\0\0\0");
        let reason = failing.cut_short().unwrap();
        assert!(reason.starts_with("disk on fire: only the first 2 of 5"), "{reason}");
    }
}
