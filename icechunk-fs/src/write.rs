use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write as _},
    os::{fd::IntoRawFd as _, unix::fs::MetadataExt as _},
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

use icechunk_storage::Generation;

use crate::{path::staging_path, read::read_with_metadata, version::generation_of};

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

/// Creates `path`, which must not exist, and writes `bytes` to it. On a
/// write error the file is removed. `AlreadyExists` can only come from the
/// create, before anything was written.
pub(crate) fn write_exclusive(path: &Path, bytes: &[u8], fsync: bool) -> io::Result<()> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    fill_or_remove(path, file, bytes, fsync, write_file)
}

/// Writes and closes `file`, created at `path`; on any error removes `path`
/// if it is still `file`.
fn fill_or_remove(
    path: &Path,
    mut file: File,
    bytes: &[u8],
    fsync: bool,
    write: impl FnOnce(&mut File, &[u8], bool) -> io::Result<()>,
) -> io::Result<()> {
    let identity = file.metadata().ok().map(|meta| (meta.dev(), meta.ino()));
    let result = match write(&mut file, bytes, fsync) {
        Ok(()) => close_checked(file),
        Err(err) => Err(err),
    };
    // A concurrent retry of the same write may have renamed a complete object
    // over `path`, which must survive. If our identity is unknown, the file
    // stays behind as an orphan or staging file for GC to sweep.
    if result.is_err()
        && let Some(identity) = identity
        && fs::symlink_metadata(path)
            .is_ok_and(|meta| (meta.dev(), meta.ino()) == identity)
    {
        let _ = fs::remove_file(path);
    }
    result
}

/// Writes `bytes` to a new staging sibling of `target` and returns its path.
fn stage(target: &Path, bytes: &[u8], fsync: bool) -> io::Result<PathBuf> {
    let staging = staging_path(target);
    write_exclusive(&staging, bytes, fsync)?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CreateOutcome {
    /// `target` holds the new content. `generation` is that of the file a
    /// fresh read sees afterwards, if it could be observed.
    Created {
        generation: Option<Generation>,
    },
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
    // No fd stays open on it: on NFS, unlinking an open file renames it
    // instead, changing the target's ctime after we observed it.
    let _ = fs::remove_file(&staging);
    match linked {
        Ok(()) => {
            sync_parent(target, fsync)?;
            // The create landed: failing to observe it must not fail it.
            let generation = generation_if_holding(target, bytes).ok().flatten();
            Ok(CreateOutcome::Created { generation })
        }
        // An NFS retry of a `link` whose reply was lost reports `EEXIST`
        // although it succeeded. Identical content means the object is this
        // version either way.
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            match generation_if_holding(target, bytes) {
                Ok(Some(generation)) => {
                    sync_parent(target, fsync)?;
                    Ok(CreateOutcome::Created { generation: Some(generation) })
                }
                Ok(None) => Ok(CreateOutcome::Exists),
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    Ok(CreateOutcome::Exists)
                }
                Err(err) => Err(err),
            }
        }
        Err(err) => Err(err),
    }
}

fn sync_parent(target: &Path, fsync: bool) -> io::Result<()> {
    match target.parent() {
        Some(parent) if fsync => fsync_dir(parent),
        _ => Ok(()),
    }
}

/// Reads `target` through a new file descriptor: the generation of the file
/// if it holds `bytes`, `None` if it holds other content.
fn generation_if_holding(target: &Path, bytes: &[u8]) -> io::Result<Option<Generation>> {
    let (current, meta) = read_with_metadata(target)?;
    Ok((current == bytes).then(|| generation_of(&meta)))
}

/// Writes `bytes` directly to `target`, which must not exist. If it does,
/// falls back to [`write_staged`].
pub(crate) fn write_new(target: &Path, bytes: &[u8], fsync: bool) -> io::Result<()> {
    ensure_parent(target)?;
    match write_exclusive(target, bytes, fsync) {
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            write_staged(target, bytes, fsync)
        }
        result => result,
    }
}

/// Fsyncs `dir`, making renames and links in it durable. Filesystems that
/// can't fsync a directory are tolerated.
pub(crate) fn fsync_dir(dir: &Path) -> io::Result<()> {
    let file = File::open(dir)?;
    tolerate_unsupported_dir_sync(file.sync_all())?;
    close_checked(file)
}

/// Maps the errors of a filesystem that doesn't support fsyncing directories
/// to success, warning once per process.
fn tolerate_unsupported_dir_sync(result: io::Result<()>) -> io::Result<()> {
    static WARNED: AtomicBool = AtomicBool::new(false);
    match result {
        Err(err)
            if err.raw_os_error().is_some_and(|code| {
                [libc::EINVAL, libc::ENOTSUP, libc::EOPNOTSUPP].contains(&code)
            }) =>
        {
            if !WARNED.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    error = %err,
                    "This filesystem doesn't support fsyncing directories; \
                     renames and links may not survive a power loss"
                );
            }
            Ok(())
        }
        result => result,
    }
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
    fn fill_or_remove_keeps_a_replaced_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x");
        let file = OpenOptions::new().write(true).create_new(true).open(&path).unwrap();
        let other = dir.path().join("other");
        fs::write(&other, b"complete").unwrap();
        fs::rename(&other, &path).unwrap();

        let err = fill_or_remove(&path, file, b"partial", false, |_, _, _| {
            Err(io::Error::other("boom"))
        })
        .unwrap_err();

        assert_eq!(err.to_string(), "boom");
        assert_eq!(fs::read(&path).unwrap(), b"complete");
    }

    #[test]
    fn write_exclusive_refuses_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x");
        write_exclusive(&path, b"first", false).unwrap();
        let err = write_exclusive(&path, b"second", false).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&path).unwrap(), b"first");
    }

    #[test]
    fn write_new_falls_back_to_staged_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a/x");
        write_new(&path, b"first", false).unwrap();
        write_new(&path, b"second", false).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"second");
        assert_eq!(fs::read_dir(dir.path().join("a")).unwrap().count(), 1);
    }

    #[test]
    fn unsupported_dir_sync_is_tolerated() {
        for code in [libc::EINVAL, libc::ENOTSUP, libc::EOPNOTSUPP] {
            tolerate_unsupported_dir_sync(Err(io::Error::from_raw_os_error(code)))
                .unwrap();
        }
        let err =
            tolerate_unsupported_dir_sync(Err(io::Error::from_raw_os_error(libc::EIO)))
                .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        tolerate_unsupported_dir_sync(Ok(())).unwrap();
    }

    fn created_generation(outcome: CreateOutcome) -> Option<Generation> {
        match outcome {
            CreateOutcome::Created { generation } => generation,
            CreateOutcome::Exists => panic!("expected Created"),
        }
    }

    #[test]
    fn create_exclusive_creates_once() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("a/repo");

        let generation =
            created_generation(create_exclusive(&target, b"first", true).unwrap());
        assert_eq!(generation, Some(generation_of(&fs::metadata(&target).unwrap())));
        assert_eq!(
            create_exclusive(&target, b"second", false).unwrap(),
            CreateOutcome::Exists
        );
        assert_eq!(fs::read(&target).unwrap(), b"first");
        assert_eq!(fs::read_dir(dir.path().join("a")).unwrap().count(), 1);
    }

    #[test]
    fn create_exclusive_with_identical_content_is_created() {
        for fsync in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let target = dir.path().join("repo");
            fs::write(&target, b"same").unwrap();

            let generation =
                created_generation(create_exclusive(&target, b"same", fsync).unwrap());
            assert_eq!(generation, Some(generation_of(&fs::metadata(&target).unwrap())));
            assert_eq!(fs::read(&target).unwrap(), b"same");
            assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
        }
    }

    #[test]
    fn create_exclusive_with_different_content_exists() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("repo");
        fs::write(&target, b"theirs").unwrap();
        let before = generation_of(&fs::metadata(&target).unwrap());

        assert_eq!(
            create_exclusive(&target, b"ours", true).unwrap(),
            CreateOutcome::Exists
        );
        assert_eq!(fs::read(&target).unwrap(), b"theirs");
        assert_eq!(generation_of(&fs::metadata(&target).unwrap()), before);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn create_exclusive_on_unreadable_target_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("repo");
        fs::create_dir_all(&target).unwrap();

        create_exclusive(&target, b"ours", false).unwrap_err();
        assert!(target.is_dir());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
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
