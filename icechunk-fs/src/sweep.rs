//! Deletes what crashed or abandoned writes leave in the repo root. See
//! "Sweeping" in design-docs/020-filesystem-storage.md.

use std::{
    collections::{BTreeMap, HashSet},
    fs, io,
    os::unix::fs::MetadataExt as _,
    path::{Path, PathBuf},
    time::SystemTime,
};

use icechunk_storage::DeleteObjectsResult;

use crate::{
    leftover::{Leftover, parse_leftover},
    list::delete_files,
    read::read_all,
    storage::is_missing,
    version::sha256_hex,
};

/// `(dev, ino)`: a lock is a hard link of its holder's lockinfo.
type Inode = (u64, u64);

struct Listed {
    path: PathBuf,
    size: u64,
    inode: Inode,
}

struct ListedLock {
    file: Listed,
    hash: String,
    old: bool,
}

/// Deletes the leftovers of crashed or abandoned writes in `root` itself
/// (not its subdirectories) whose mtime is before `cutoff`: tickets, probe
/// files and staging files whatever their content, lockinfos unless linked
/// as a lock that is kept, and locks whose hash is not the current content
/// of their target. Lock files are listed before their targets are read: a
/// lock listed before the read was taken on a version current at or before
/// the listing, so if the read shows another hash (or no target), that
/// family can never be used again. Unlinking a lockinfo still linked as a
/// kept lock would refresh that lock's ctime and delay stealing it. With
/// `dry_run`, deletes nothing and reports what it would delete. A
/// missing `root` sweeps nothing.
pub(crate) fn sweep_root(
    root: &Path,
    cutoff: SystemTime,
    dry_run: bool,
) -> io::Result<DeleteObjectsResult> {
    let entries = match fs::read_dir(root) {
        // root exists
        Ok(entries) => entries,
        // no root: nothing to sweep
        Err(err) if is_missing(&err) => return Ok(DeleteObjectsResult::default()),
        // root unreadable
        Err(err) => return Err(err),
    };
    let mut candidates = Vec::new();
    let mut lockinfos = Vec::new();
    // target name -> every lock on it, old or young, of any hash
    let mut locks: BTreeMap<String, Vec<ListedLock>> = BTreeMap::new();
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(kind) = file_name.to_str().and_then(parse_leftover) else {
            continue;
        };
        match entry.file_type() {
            // regular file, including a lock (hard link)
            Ok(file_type) if file_type.is_file() => {}
            // directory, symlink (not followed) or special file: the protocols
            // never create one, so it isn't ours to delete
            Ok(_) => continue,
            // deleted since the listing
            Err(err) if is_missing(&err) => continue,
            // can't stat it
            Err(err) => return Err(err),
        }
        let metadata = match entry.metadata() {
            // still there
            Ok(metadata) => metadata,
            // deleted since the listing
            Err(err) if is_missing(&err) => continue,
            // can't stat it
            Err(err) => return Err(err),
        };
        let old = metadata.modified()? < cutoff;
        let file = Listed {
            path: entry.path(),
            size: metadata.len(),
            inode: (metadata.dev(), metadata.ino()),
        };
        match kind {
            // any lock, old or young: decided per target below. Young locks are
            // listed too: their lockinfos must be kept.
            Leftover::Lock { target, hash } => {
                locks.entry(target.to_string()).or_default().push(ListedLock {
                    file,
                    hash: hash.to_string(),
                    old,
                });
            }
            // any other young leftover: kept
            _ if !old => {}
            // old lockinfo: deleted unless linked as a kept lock
            Leftover::LockInfo => lockinfos.push(file),
            // old staging file, ticket or probe file: deleted
            Leftover::Staging | Leftover::Ticket | Leftover::Probe => {
                candidates.push(file);
            }
        }
    }
    let mut kept_locks = HashSet::new();
    for (target, target_locks) in locks {
        let target = root.join(target);
        let current = match read_all(&target) {
            // target exists: its current hash
            Ok(bytes) => Some(sha256_hex(&bytes)),
            // no target: no lock hash is current
            Err(err) if is_missing(&err) => None,
            // target unreadable: keep all its locks
            Err(err) => {
                tracing::warn!(
                    path = %target.display(),
                    error = %err,
                    "Cannot read lock target, keeping its locks"
                );
                kept_locks.extend(target_locks.iter().map(|lock| lock.file.inode));
                continue;
            }
        };
        for lock in target_locks {
            if lock.old && current.as_deref() != Some(lock.hash.as_str()) {
                candidates.push(lock.file);
            } else {
                kept_locks.insert(lock.file.inode);
            }
        }
    }
    candidates
        .extend(lockinfos.into_iter().filter(|info| !kept_locks.contains(&info.inode)));
    if dry_run {
        return Ok(DeleteObjectsResult {
            deleted_objects: candidates.len() as u64,
            deleted_bytes: candidates.iter().map(|file| file.size).sum(),
        });
    }
    Ok(delete_files(candidates.into_iter().map(|file| (file.path, file.size)).collect()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        fs::{self, File},
        path::PathBuf,
        time::{Duration, SystemTime},
    };

    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::{
        lock::ProtocolPaths,
        path::staging_path,
        probe::{probe_link_name, probe_name},
        version::sha256_hex,
    };

    const HOUR: Duration = Duration::from_secs(3600);

    fn cutoff() -> SystemTime {
        SystemTime::now() - HOUR
    }

    fn old(path: &Path) {
        File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(SystemTime::now() - 2 * HOUR)
            .unwrap();
    }

    fn names(dir: &Path) -> BTreeSet<String> {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    fn set(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn write(path: &Path, content: &[u8]) -> PathBuf {
        fs::write(path, content).unwrap();
        path.to_path_buf()
    }

    /// `repo` = `v1` and the 7 leftovers of the first tests, returned with
    /// their total size.
    fn leftovers(root: &Path) -> (Vec<PathBuf>, u64) {
        let repo = write(&root.join("repo"), b"v1");
        let paths = ProtocolPaths::new(&repo).unwrap();
        let dead = sha256_hex(b"v0");
        let files = vec![
            write(&paths.ticket(Uuid::new_v4()), b"ticket"),
            write(&paths.lockinfo(Uuid::new_v4()), b"lockinfo!"),
            write(&root.join(probe_name(Uuid::new_v4())), b"p"),
            write(&root.join(probe_link_name(Uuid::new_v4())), b"pl"),
            write(&staging_path(&repo), b"staged bytes"),
            write(&paths.lock(&dead, 0), b"lock 0"),
            write(&paths.lock(&dead, 1), b"lock one"),
        ];
        let bytes = files.iter().map(|file| fs::metadata(file).unwrap().len()).sum();
        (files, bytes)
    }

    #[test]
    fn old_leftovers_are_swept() {
        let root = TempDir::new().unwrap();
        let (files, bytes) = leftovers(root.path());
        files.iter().for_each(|file| old(file));
        let res = sweep_root(root.path(), cutoff(), false).unwrap();
        assert_eq!(names(root.path()), set(&["repo"]));
        assert_eq!(res.deleted_objects, 7);
        assert_eq!(res.deleted_bytes, bytes);
    }

    #[test]
    fn fresh_leftovers_are_kept() {
        let root = TempDir::new().unwrap();
        leftovers(root.path());
        let before = names(root.path());
        let res = sweep_root(root.path(), cutoff(), false).unwrap();
        assert_eq!(names(root.path()), before);
        assert_eq!(res, DeleteObjectsResult::default());
    }

    #[test]
    fn current_lock_is_never_swept() {
        let root = TempDir::new().unwrap();
        let repo = write(&root.path().join("repo"), b"v1");
        let lock = write(
            &ProtocolPaths::new(&repo).unwrap().lock(&sha256_hex(b"v1"), 0),
            b"lock",
        );
        old(&lock);
        let before = names(root.path());
        let res = sweep_root(root.path(), cutoff(), false).unwrap();
        assert_eq!(names(root.path()), before);
        assert_eq!(res, DeleteObjectsResult::default());
    }

    #[test]
    fn locks_of_a_missing_target_are_swept() {
        let root = TempDir::new().unwrap();
        let paths = ProtocolPaths::new(&root.path().join("config.yaml")).unwrap();
        let hash = sha256_hex(b"c0");
        let stale = write(&paths.lock(&hash, 0), b"lock");
        old(&stale);
        let fresh = write(&paths.lock(&hash, 1), b"lock");
        let res = sweep_root(root.path(), cutoff(), false).unwrap();
        assert!(!stale.exists());
        assert!(fresh.exists());
        assert_eq!(res.deleted_objects, 1);
    }

    #[test]
    fn locks_of_an_unreadable_target_are_kept() {
        let root = TempDir::new().unwrap();
        let repo = root.path().join("repo");
        fs::create_dir_all(&repo).unwrap();
        let lock = write(
            &ProtocolPaths::new(&repo).unwrap().lock(&sha256_hex(b"v0"), 0),
            b"lock",
        );
        old(&lock);
        let res = sweep_root(root.path(), cutoff(), false).unwrap();
        assert!(lock.exists());
        assert_eq!(res, DeleteObjectsResult::default());
    }

    /// An old lockinfo of `repo`, hard-linked as `repo.lock.<hash>.0`, as a
    /// writer that crashed after taking the lock leaves it.
    fn crashed_holder(repo: &Path, hash: &str) -> (PathBuf, PathBuf) {
        let paths = ProtocolPaths::new(repo).unwrap();
        let id = Uuid::new_v4();
        let lockinfo = write(&paths.lockinfo(id), id.to_string().as_bytes());
        old(&lockinfo);
        let lock = paths.lock(hash, 0);
        fs::hard_link(&lockinfo, &lock).unwrap();
        (lockinfo, lock)
    }

    fn ctime(path: &Path) -> (i64, i64) {
        let metadata = fs::metadata(path).unwrap();
        (metadata.ctime(), metadata.ctime_nsec())
    }

    #[test]
    fn lockinfo_linked_to_a_kept_lock_is_kept() {
        let root = TempDir::new().unwrap();
        let repo = write(&root.path().join("repo"), b"v1");
        let (lockinfo, lock) = crashed_holder(&repo, &sha256_hex(b"v1"));
        let before = ctime(&lock);
        let res = sweep_root(root.path(), cutoff(), false).unwrap();
        assert!(lockinfo.exists());
        assert!(lock.exists());
        assert_eq!(ctime(&lock), before);
        assert_eq!(res, DeleteObjectsResult::default());
    }

    #[test]
    fn lockinfo_linked_to_a_dead_lock_is_swept() {
        let root = TempDir::new().unwrap();
        let repo = write(&root.path().join("repo"), b"v1");
        let (lockinfo, lock) = crashed_holder(&repo, &sha256_hex(b"v0"));
        let res = sweep_root(root.path(), cutoff(), false).unwrap();
        assert_eq!(names(root.path()), set(&["repo"]));
        assert!(!lockinfo.exists() && !lock.exists());
        assert_eq!(res.deleted_objects, 2);
    }

    #[test]
    fn lockinfo_linked_to_a_lock_of_an_unreadable_target_is_kept() {
        let root = TempDir::new().unwrap();
        let repo = root.path().join("repo");
        fs::create_dir_all(&repo).unwrap();
        let (lockinfo, lock) = crashed_holder(&repo, &sha256_hex(b"v0"));
        let before = ctime(&lock);
        let res = sweep_root(root.path(), cutoff(), false).unwrap();
        assert!(lockinfo.exists());
        assert!(lock.exists());
        assert_eq!(ctime(&lock), before);
        assert_eq!(res, DeleteObjectsResult::default());
    }

    #[test]
    fn other_keys_follow_the_same_rules() {
        let root = TempDir::new().unwrap();
        let config = write(&root.path().join("config.yaml"), b"c1");
        let paths = ProtocolPaths::new(&config).unwrap();
        let dead = write(&paths.lock(&sha256_hex(b"c0"), 0), b"lock");
        let current = write(&paths.lock(&sha256_hex(b"c1"), 0), b"lock");
        let ticket = write(&paths.ticket(Uuid::new_v4()), b"ticket");
        [&config, &dead, &current, &ticket].into_iter().for_each(|file| old(file));
        let res = sweep_root(root.path(), cutoff(), false).unwrap();
        assert!(!dead.exists());
        assert!(current.exists());
        assert!(!ticket.exists());
        assert!(config.exists());
        assert_eq!(res.deleted_objects, 2);
    }

    #[test]
    fn objects_and_subdirectories_are_untouched() {
        let root = TempDir::new().unwrap();
        let chunks = root.path().join("chunks");
        fs::create_dir_all(&chunks).unwrap();
        let files = [
            write(&root.path().join("repo"), b"v1"),
            write(&staging_path(&chunks.join("ABC")), b"staged"),
            write(&chunks.join(format!("DEF.lockinfo.{}", Uuid::new_v4())), b"info"),
            write(&root.path().join("notes.txt"), b"notes"),
        ];
        files.iter().for_each(|file| old(file));
        let dir = root.path().join(format!("repo.lockinfo.{}", Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let res = sweep_root(root.path(), cutoff(), false).unwrap();
        assert!(files.iter().all(|file| file.exists()));
        assert!(dir.is_dir());
        assert_eq!(res, DeleteObjectsResult::default());
    }

    #[test]
    fn dry_run_deletes_nothing() {
        let root = TempDir::new().unwrap();
        let (files, bytes) = leftovers(root.path());
        files.iter().for_each(|file| old(file));
        let before = names(root.path());
        let res = sweep_root(root.path(), cutoff(), true).unwrap();
        assert_eq!(names(root.path()), before);
        assert_eq!(res.deleted_objects, 7);
        assert_eq!(res.deleted_bytes, bytes);
    }

    #[test]
    fn missing_root_sweeps_nothing() {
        let root = TempDir::new().unwrap();
        let res = sweep_root(&root.path().join("missing"), cutoff(), false).unwrap();
        assert_eq!(res, DeleteObjectsResult::default());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn non_utf8_names_are_ignored() {
        use std::{ffi::OsStr, os::unix::ffi::OsStrExt as _};
        let root = TempDir::new().unwrap();
        let file =
            write(&root.path().join(OsStr::from_bytes(b"repo.lockinfo.\xff")), b"x");
        old(&file);
        let res = sweep_root(root.path(), cutoff(), false).unwrap();
        assert!(file.exists());
        assert_eq!(res, DeleteObjectsResult::default());
    }
}
