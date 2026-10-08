use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write as _},
    os::fd::IntoRawFd as _,
    path::{Path, PathBuf},
};

use crate::path::staging_path;

/// Closes `file` and returns the error from `close(2)`, which dropping a
/// `File` ignores. On NFS, write errors such as running out of space or quota
/// may only be reported at close.
pub(crate) fn close_checked(file: File) -> io::Result<()> {
    let fd = file.into_raw_fd();
    // SAFETY: `fd` came from `into_raw_fd`, so we own it and nothing else closes it.
    #[expect(unsafe_code)]
    let rc = unsafe { libc::close(fd) };
    // Never retry: after a failed close the fd is released on Linux and macOS,
    // and its number may already belong to another thread's file.
    if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

pub(crate) fn write_file(file: &mut File, bytes: &[u8], fsync: bool) -> io::Result<()> {
    file.write_all(bytes)?;
    if fsync { file.sync_all() } else { Ok(()) }
}

pub(crate) fn ensure_parent(target: &Path) -> io::Result<()> {
    target.parent().map_or(Ok(()), fs::create_dir_all)
}

/// Writes and closes `file`, created at `path`; on any error removes `path`.
fn fill_or_remove(
    path: &Path,
    mut file: File,
    bytes: &[u8],
    fsync: bool,
    write: impl FnOnce(&mut File, &[u8], bool) -> io::Result<()>,
) -> io::Result<()> {
    let result = match write(&mut file, bytes, fsync) {
        Ok(()) => close_checked(file),
        Err(err) => Err(err),
    };
    if result.is_err() {
        let _ = fs::remove_file(path);
    }
    result
}

/// Writes `bytes` to a new staging sibling of `target` and returns its path.
fn stage(target: &Path, bytes: &[u8], fsync: bool) -> io::Result<PathBuf> {
    let staging = staging_path(target);
    let file = OpenOptions::new().write(true).create_new(true).open(&staging)?;
    fill_or_remove(&staging, file, bytes, fsync, write_file)?;
    Ok(staging)
}

/// Writes `bytes` to a staging sibling of `target`, then renames it into place.
pub(crate) fn write_staged(target: &Path, bytes: &[u8], fsync: bool) -> io::Result<()> {
    ensure_parent(target)?;
    let staging = stage(target, bytes, fsync)?;
    fs::rename(&staging, target).inspect_err(|_| {
        let _ = fs::remove_file(&staging);
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreateOutcome {
    Created,
    Exists,
}

/// Writes `bytes` to a staging sibling of `target`, then links it into place
/// if `target` doesn't exist. The staging file is always removed.
pub(crate) fn create_exclusive(
    target: &Path,
    bytes: &[u8],
    fsync: bool,
) -> io::Result<CreateOutcome> {
    ensure_parent(target)?;
    let staging = stage(target, bytes, fsync)?;
    let linked = fs::hard_link(&staging, target);
    // The link alone decides the outcome; GC sweeps a staging file left behind.
    let _ = fs::remove_file(&staging);
    match linked {
        Ok(()) => {
            if fsync && let Some(parent) = target.parent() {
                fsync_dir(parent)?;
            }
            Ok(CreateOutcome::Created)
        }
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            Ok(CreateOutcome::Exists)
        }
        Err(err) => Err(err),
    }
}

/// Writes `bytes` directly to `target`, which must not exist. If it does,
/// falls back to [`write_staged`].
pub(crate) fn write_new(target: &Path, bytes: &[u8], fsync: bool) -> io::Result<()> {
    ensure_parent(target)?;
    match OpenOptions::new().write(true).create_new(true).open(target) {
        Ok(file) => fill_or_remove(target, file, bytes, fsync, write_file),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            write_staged(target, bytes, fsync)
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn fsync_dir(dir: &Path) -> io::Result<()> {
    let file = File::open(dir)?;
    file.sync_all()?;
    close_checked(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fill_or_remove_removes_on_write_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x");
        let file = OpenOptions::new().write(true).create_new(true).open(&path).unwrap();

        let err = fill_or_remove(&path, file, b"0123456789", true, |file, bytes, _| {
            file.write_all(&bytes[..bytes.len() / 2])?;
            Err(io::Error::other("boom"))
        })
        .unwrap_err();

        assert_eq!(err.to_string(), "boom");
        assert!(!path.exists());
    }

    #[test]
    fn create_exclusive_creates_once() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("a/repo");

        assert_eq!(
            create_exclusive(&target, b"first", true).unwrap(),
            CreateOutcome::Created
        );
        assert_eq!(
            create_exclusive(&target, b"second", false).unwrap(),
            CreateOutcome::Exists
        );
        assert_eq!(fs::read(&target).unwrap(), b"first");
        assert_eq!(fs::read_dir(dir.path().join("a")).unwrap().count(), 1);
    }

    #[test]
    fn close_checked_closes_new_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = File::create(dir.path().join("x")).unwrap();
        close_checked(file).unwrap();
    }

    #[test]
    fn fsync_dir_syncs_directory() {
        let dir = tempfile::tempdir().unwrap();
        fsync_dir(dir.path()).unwrap();
    }
}
