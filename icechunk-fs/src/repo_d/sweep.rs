//! The GC's sweep of what the protocol leaves behind, see "Sweep" in
//! design-docs/020-filesystem-storage.md.

use std::{
    fs::{self, DirEntry, Metadata, ReadDir},
    io,
    path::{Path, PathBuf},
    time::SystemTime,
};

use icechunk_storage::DeleteObjectsResult;

use super::{RepoD, discard_tmp_dir, is_dead};
use crate::{
    hooks::{ProtocolHooks, Sabotage},
    names::{REPO_D, RepoDName, RootName, parse_repo_d_name, parse_root_name},
    nudge::nudge,
    path::is_staging_name,
    storage::is_missing,
};

enum Leftover {
    /// `stage.*`, `mirror.*` or `<name>.staging.<id>`.
    Staging(PathBuf),
    /// A version file the marker proves dead.
    Version(PathBuf),
    /// `repo.d.tmp.*`, one object.
    TmpDir(PathBuf),
}

/// Deletes, from one listing each of `root` and `repo.d`:
/// - the version files that the marker of the listing of `repo.d` proves
///   dead, whatever their age: `<i>.<x>` with `i < N`, and `<N>.<x>` with
///   `x != h`. Files above `N` belong to attempts in flight.
/// - staging files and creation directories last modified before `cutoff`:
///   `repo.d/stage.*` and `repo.d/mirror.*`, and in the root `repo.d.tmp.*`
///   and `<name>.staging.<id>`, `repo.staging.<id>` included.
///   Deleting them affects liveness only.
///
/// Everything else is kept, and other subdirectories aren't visited. A
/// missing root or `repo.d` sweeps what exists. With `dry_run`, deletes
/// nothing and reports what it would delete. Failures to delete a file are
/// logged and left out of the result.
// TODO: collect the staging files that crashes leave in `snapshots/`,
// `overwritten/` and the other object directories. Nothing deletes them yet:
// GC's object walk ignores non-canonical names and never visits
// `overwritten/`, and this sweep visits only the root and `repo.d`.
pub(crate) fn sweep(
    root: &Path,
    cutoff: SystemTime,
    dry_run: bool,
    hooks: &dyn ProtocolHooks,
) -> io::Result<DeleteObjectsResult> {
    let mut leftovers = Vec::new();
    for entry in read_dir_if_exists(root)?.into_iter().flatten() {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else { continue };
        match parse_root_name(name) {
            Some(RootName::TmpDir) => {
                if old_metadata(&entry, cutoff, Metadata::is_dir).is_some() {
                    let path = entry.path();
                    let size = dir_size(&path);
                    leftovers.push((Leftover::TmpDir(path), size));
                }
            }
            Some(RootName::RepoD) => {}
            None if is_staging_name(&file_name) => {
                if let Some(metadata) = old_metadata(&entry, cutoff, Metadata::is_file) {
                    leftovers.push((Leftover::Staging(entry.path()), metadata.len()));
                }
            }
            None => {}
        }
    }

    let dir = root.join(REPO_D);
    nudge(&dir);
    let mut versions = Vec::new();
    let mut markers = Vec::new();
    for entry in read_dir_if_exists(&dir)?.into_iter().flatten() {
        let entry = entry?;
        match entry.file_name().to_str().and_then(parse_repo_d_name) {
            Some(RepoDName::Content(version)) => {
                if let Some(metadata) = file_metadata(&entry) {
                    versions.push((version, entry.path(), metadata.len()));
                }
            }
            Some(RepoDName::Stage(_) | RepoDName::Mirror(_)) => {
                if let Some(metadata) = old_metadata(&entry, cutoff, Metadata::is_file) {
                    leftovers.push((Leftover::Staging(entry.path()), metadata.len()));
                }
            }
            Some(RepoDName::Marker(marker)) => markers.push(marker),
            None => {}
        }
    }
    if let Some(marker) = RepoD::highest(&markers) {
        let current = &marker.version;
        let delete_above = hooks.sabotaged(Sabotage::DeleteAboveMarker);
        leftovers.extend(
            versions
                .into_iter()
                .filter(|(version, ..)| {
                    is_dead(version, current)
                        || (delete_above && version.n() > current.n())
                })
                .map(|(_, path, size)| (Leftover::Version(path), size)),
        );
    }

    Ok(leftovers.into_iter().fold(
        DeleteObjectsResult::default(),
        |mut res, (leftover, size)| {
            if dry_run || delete(root, hooks, &leftover) {
                res.deleted_objects += 1;
                res.deleted_bytes += size;
            }
            res
        },
    ))
}

/// Whether `leftover` is gone. A file already missing counts as deleted; a
/// creation directory that is gone may have become `repo.d`, and doesn't.
fn delete(root: &Path, hooks: &dyn ProtocolHooks, leftover: &Leftover) -> bool {
    let (path, result) = match leftover {
        Leftover::Staging(path) => (path, fs::remove_file(path)),
        Leftover::Version(path) => (path, hooks.unlink(path)),
        Leftover::TmpDir(path) => match discard_tmp_dir(root, path) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => return false,
            result => (path, result),
        },
    };
    match result {
        Err(err) if !is_missing(&err) => {
            tracing::error!(path = %path.display(), error = %err, "Error deleting object");
            false
        }
        _ => true,
    }
}

fn read_dir_if_exists(dir: &Path) -> io::Result<Option<ReadDir>> {
    match fs::read_dir(dir) {
        Ok(entries) => Ok(Some(entries)),
        Err(err) if is_missing(&err) => Ok(None),
        Err(err) => Err(err),
    }
}

/// The metadata of a regular file, without following a symlink. `None` if it
/// is something else or can't be read.
fn file_metadata(entry: &DirEntry) -> Option<Metadata> {
    entry_metadata(entry).filter(Metadata::is_file)
}

/// The metadata of `entry`, if it is of the `kind` (`Metadata::is_file` or
/// `Metadata::is_dir`) and was last modified before `cutoff`. What can't be
/// read is kept.
fn old_metadata(
    entry: &DirEntry,
    cutoff: SystemTime,
    kind: fn(&Metadata) -> bool,
) -> Option<Metadata> {
    entry_metadata(entry)
        .filter(kind)
        .filter(|metadata| metadata.modified().is_ok_and(|modified| modified < cutoff))
}

fn entry_metadata(entry: &DirEntry) -> Option<Metadata> {
    entry
        .metadata()
        .inspect_err(|err| {
            if !is_missing(err) {
                tracing::debug!(%err, path = %entry.path().display(), "could not stat a leftover");
            }
        })
        .ok()
}

/// Best effort: the total size of the regular files in `dir`.
fn dir_size(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok()?.metadata().ok())
                .filter(Metadata::is_file)
                .map(|metadata| metadata.len())
                .sum()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        fs::{self, File},
        path::PathBuf,
        time::Duration,
    };

    use uuid::Uuid;

    use super::{super::testing::*, *};
    use crate::{
        hooks::{NoHooks, Sabotage},
        names::{mirror_name, stage_name, tmp_dir_name},
        path::staging_path,
    };

    const HOUR: Duration = Duration::from_secs(3600);

    fn cutoff() -> SystemTime {
        SystemTime::now() - HOUR
    }

    /// Sets the mtime of a file or directory to before the cutoff.
    fn age(path: &Path) {
        File::open(path).unwrap().set_modified(SystemTime::now() - 2 * HOUR).unwrap();
    }

    fn write(path: PathBuf, bytes: &[u8]) -> PathBuf {
        fs::write(&path, bytes).unwrap();
        path
    }

    fn old(path: PathBuf, bytes: &[u8]) -> PathBuf {
        let path = write(path, bytes);
        age(&path);
        path
    }

    fn size(paths: &[&PathBuf]) -> u64 {
        paths.iter().map(|path| fs::metadata(path).unwrap().len()).sum()
    }

    /// A `repo.d.tmp.*` holding a version and its marker.
    fn tmp_dir(root: &Path, bytes: &[u8]) -> PathBuf {
        let tmp = root.join(tmp_dir_name(Uuid::new_v4()));
        fs::create_dir_all(&tmp).unwrap();
        let v0 = version(0, bytes);
        write(tmp.join(v0.file_name()), bytes);
        write(tmp.join(latest(&v0)), b"");
        tmp
    }

    fn all_names(root: &Path) -> BTreeSet<String> {
        let mut all = names(root);
        if root.join("repo.d").is_dir() {
            all.extend(
                names(&root.join("repo.d")).into_iter().map(|n| format!("repo.d/{n}")),
            );
        }
        all
    }

    struct Layout {
        kept: BTreeSet<String>,
        deleted: DeleteObjectsResult,
    }

    /// Every kind of name in the root and in `repo.d`, the marker at `3.<h3>`.
    fn layout(root: &Path) -> Layout {
        let dir = root.join("repo.d");
        fs::create_dir_all(&dir).unwrap();
        let v3 = version(3, &repo_info(3));
        let marker = old(dir.join(latest(&v3)), b"");
        let current = old(dir.join(v3.file_name()), &repo_info(3));
        // Dead whatever their age.
        let dead = [
            write(dir.join(version(0, &repo_info(10)).file_name()), &repo_info(10)),
            write(dir.join(version(2, &repo_info(11)).file_name()), &repo_info(11)),
            write(dir.join(version(3, &repo_info(13)).file_name()), &repo_info(13)),
        ];
        // In flight: above the marker.
        let above = [
            old(dir.join(version(4, &repo_info(14)).file_name()), &repo_info(14)),
            old(dir.join(version(8, &repo_info(15)).file_name()), &repo_info(15)),
        ];
        let old_stage = old(dir.join(stage_name(Uuid::new_v4())), b"staged");
        let young_stage = write(dir.join(stage_name(Uuid::new_v4())), b"staged");
        let old_mirror = old(dir.join(mirror_name(Uuid::new_v4())), b"mirror");
        let young_mirror = write(dir.join(mirror_name(Uuid::new_v4())), b"mirror");
        let nfs = old(dir.join(".nfs0002"), b"n");
        let unknown = old(dir.join("unknown"), b"u");

        let mirror = old(root.join("repo"), &repo_info(3));
        // Unknown names, whatever they once meant.
        let legacy = old(root.join(format!("repo.legacy.{}", Uuid::new_v4())), b"legacy");
        let stub = old(root.join(format!("repo.stub.{}", Uuid::new_v4())), b"stub");
        let old_tmp = tmp_dir(root, &repo_info(16));
        age(&old_tmp);
        let old_tmp_files: Vec<PathBuf> =
            fs::read_dir(&old_tmp).unwrap().map(|e| e.unwrap().path()).collect();
        let young_tmp = tmp_dir(root, &repo_info(17));
        let config = old(root.join("config.yaml"), b"config");
        let old_staging = old(staging_path(&config), b"staged config");
        let young_staging = write(staging_path(&config), b"staged config");
        let root_nfs = old(root.join(".nfs0001"), b"n");
        let root_unknown = old(root.join("unknown"), b"u");
        let chunks = root.join("chunks");
        fs::create_dir_all(&chunks).unwrap();
        // Subdirectories other than `repo.d` aren't visited.
        let nested = old(staging_path(&chunks.join("ABC")), b"nested");

        let mut deleted: Vec<&PathBuf> = dead.iter().collect();
        deleted.extend([&old_stage, &old_mirror, &old_staging]);
        let tmp_files: Vec<&PathBuf> = old_tmp_files.iter().collect();
        let deleted = DeleteObjectsResult {
            // The tmp directory is one object.
            deleted_objects: deleted.len() as u64 + 1,
            deleted_bytes: size(&deleted) + size(&tmp_files),
        };
        let name = |path: &PathBuf| {
            path.strip_prefix(root).unwrap().to_str().unwrap().to_string()
        };
        let kept = [
            &marker,
            &current,
            &above[0],
            &above[1],
            &young_stage,
            &young_mirror,
            &nfs,
            &unknown,
            &mirror,
            &legacy,
            &stub,
            &young_tmp,
            &config,
            &young_staging,
            &root_nfs,
            &root_unknown,
            &chunks,
        ]
        .into_iter()
        .map(name)
        .chain(["repo.d".to_string()])
        .collect();
        assert!(nested.exists());
        Layout { kept, deleted }
    }

    #[test]
    fn sweep_deletes_dead_versions_and_old_leftovers_only() {
        let root = tempfile::tempdir().unwrap();
        let Layout { kept, deleted } = layout(root.path());
        let res = sweep(root.path(), cutoff(), false, &NoHooks).unwrap();
        assert_eq!(all_names(root.path()), kept);
        assert_eq!(res, deleted);
        assert_eq!(names(&root.path().join("chunks")).len(), 1);
    }

    #[test]
    fn dry_run_counts_what_a_sweep_deletes_and_deletes_nothing() {
        let root = tempfile::tempdir().unwrap();
        let Layout { deleted, .. } = layout(root.path());
        let before = all_names(root.path());
        let res = sweep(root.path(), cutoff(), true, &NoHooks).unwrap();
        assert_eq!(all_names(root.path()), before);
        assert_eq!(res, deleted);
        assert_eq!(sweep(root.path(), cutoff(), false, &NoHooks).unwrap(), deleted);
    }

    #[test]
    fn without_a_marker_no_version_is_deleted() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("repo.d");
        fs::create_dir_all(&dir).unwrap();
        let versions = [
            old(dir.join(version(0, &repo_info(10)).file_name()), &repo_info(10)),
            old(dir.join(version(1, &repo_info(12)).file_name()), &repo_info(12)),
        ];
        let stage = old(dir.join(stage_name(Uuid::new_v4())), b"staged");
        let staging = old(staging_path(&root.path().join("repo")), b"s");
        let res = sweep(root.path(), cutoff(), false, &NoHooks).unwrap();
        assert!(versions.iter().all(|path| path.exists()));
        assert!(!stage.exists() && !staging.exists());
        assert_eq!(res.deleted_objects, 2);
    }

    #[test]
    fn torn_listing_takes_the_highest_marker() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = root.path().join("repo.d");
        let v1 = version(1, &repo_info(1));
        write(dir.join(v1.file_name()), &repo_info(1));
        write(dir.join(latest(&v1)), b"");
        sweep(root.path(), cutoff(), false, &NoHooks).unwrap();
        assert_eq!(names(&dir), set([latest(&v0), v1.file_name(), latest(&v1)]));
    }

    #[test]
    fn missing_root_or_repo_d_sweeps_what_exists() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing");
        assert_eq!(
            sweep(&missing, cutoff(), false, &NoHooks).unwrap(),
            DeleteObjectsResult::default()
        );
        let staging = old(staging_path(&root.path().join("repo")), b"s");
        let res = sweep(root.path(), cutoff(), false, &NoHooks).unwrap();
        assert!(!staging.exists());
        assert_eq!(res, DeleteObjectsResult { deleted_objects: 1, deleted_bytes: 1 });
    }

    #[test]
    fn repo_d_that_is_not_a_directory_is_kept() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("repo.d"), b"").unwrap();
        let staging = old(staging_path(&root.path().join("repo")), b"s");
        let res = sweep(root.path(), cutoff(), false, &NoHooks).unwrap();
        assert_eq!(res.deleted_objects, 1);
        assert!(!staging.exists());
        assert!(root.path().join("repo.d").is_file());
    }

    #[test]
    fn version_deletions_go_through_the_hooks() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = root.path().join("repo.d");
        let above = write(dir.join(version(1, &repo_info(1)).file_name()), &repo_info(1));
        let hooks = TestHooks::default().sabotage(Sabotage::DeleteAboveMarker);
        let res = sweep(root.path(), cutoff(), false, &hooks).unwrap();
        assert!(!above.exists());
        assert_eq!(res.deleted_objects, 1);
        assert_eq!(names(&dir), set([v0.file_name(), latest(&v0)]));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn non_utf8_names_are_kept() {
        use std::{ffi::OsStr, os::unix::ffi::OsStrExt as _};
        let root = tempfile::tempdir().unwrap();
        let file = old(
            root.path().join(OsStr::from_bytes(b"repo.staging.\xff")),
            &repo_info(14),
        );
        let res = sweep(root.path(), cutoff(), false, &NoHooks).unwrap();
        assert!(file.exists());
        assert_eq!(res, DeleteObjectsResult::default());
    }
}
