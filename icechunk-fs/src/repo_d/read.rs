//! Reads of `repo`: `read_latest` and the version files themselves.

use std::{fs, path::Path, time::SystemTime};

use bytes::Bytes;

use super::{Listed, ProtocolError, RepoD, Step};
use crate::{
    names::{Hash, Version},
    nudge::nudge,
    read::read_all,
    storage::is_missing,
};

/// The current version of `repo` and its bytes.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Current {
    pub(crate) bytes: Bytes,
    pub(crate) version: Version,
}

/// What [`RepoD::read_since`] found.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Since {
    /// The listed marker is the previous version, or lags behind it.
    NotModified,
    Modified(Current),
}

impl RepoD<'_> {
    /// The current version and its bytes, `None` if `repo.d` doesn't exist.
    /// Lists again while the listing shows no marker or its version file
    /// can't be read, which happens when a commit lands during the listing or
    /// deletes the version under us.
    pub(crate) fn read_latest(&self) -> Result<Option<Current>, ProtocolError> {
        self.read_listed(|_| None, |current| current)
    }

    /// [`RepoD::read_latest`], without reading when the listed marker is
    /// `previous` or lags behind it.
    pub(crate) fn read_since(
        &self,
        previous: &Version,
    ) -> Result<Option<Since>, ProtocolError> {
        self.read_listed(
            |version| {
                (version == previous || version.n() < previous.n())
                    .then_some(Since::NotModified)
            },
            Since::Modified,
        )
    }

    /// Reads the version of the listed marker as `read`, unless `unread`
    /// answers from the version alone.
    fn read_listed<T>(
        &self,
        unread: impl Fn(&Version) -> Option<T>,
        read: impl Fn(Current) -> T,
    ) -> Result<Option<T>, ProtocolError> {
        let dir = self.dir();
        // Every read found the same listed version missing: a copy taken
        // without the recipe can name a version it doesn't hold, while a
        // live repository's marker moves on.
        let mut missing_version: Option<Version> = None;
        let mut never_appeared = true;
        self.with_listing(|marker| {
            let Some(marker) = marker else {
                return Ok(Step::Done(None));
            };
            let version = &marker.version;
            if let Some(unread) = unread(version) {
                return Ok(Step::Done(Some(unread)));
            }
            Ok(match read_version_file(&dir, version) {
                Ok(bytes) => {
                    Step::Done(Some(read(Current { bytes, version: version.clone() })))
                }
                Err(err) => {
                    tracing::debug!(?err, "could not read the listed version of repo");
                    never_appeared &=
                        matches!(&err, ProtocolError::Io(err) if is_missing(err))
                            && *missing_version.get_or_insert_with(|| version.clone())
                                == *version;
                    Step::Retry(match err {
                        ProtocolError::Corrupt(msg) => msg,
                        ProtocolError::Io(err) if never_appeared => format!(
                            "could not read {}: {err}; if the repository at {} is a copy, \
                             it may not have been taken as \"Copying a repository\" in \
                             the filesystem layout specification says",
                            version.file_name(),
                            self.root.display()
                        ),
                        ProtocolError::Io(err) => {
                            format!("could not read {}: {err}", version.file_name())
                        }
                        err => format!("{err:?}"),
                    })
                }
            })
        })
    }

    /// The modification time of the current version's file, `None` if
    /// `repo.d` doesn't exist. Lists again while the file is gone.
    pub(crate) fn current_modified(&self) -> Result<Option<SystemTime>, ProtocolError> {
        let dir = self.dir();
        self.with_listing(|marker| {
            let Some(marker) = marker else {
                return Ok(Step::Done(None));
            };
            let name = marker.version.file_name();
            match fs::metadata(dir.join(&name)).and_then(|metadata| metadata.modified()) {
                Ok(modified) => Ok(Step::Done(Some(modified))),
                Err(err) if is_missing(&err) => {
                    Ok(Step::Retry(format!("{name} is gone")))
                }
                Err(err) => Err(err.into()),
            }
        })
    }

    /// For `copy_object`, which backs up `base` before a change from it:
    /// `None` when a listing of `repo.d` shows a marker above `base`, as a
    /// copy-if-match of a stale version fails, or when
    /// [`RepoD::read_version`] finds no bytes. Only a marker above `base`
    /// counts: a listing without `base`'s marker may be stale.
    pub(crate) fn read_backup_source(
        &self,
        base: &Version,
    ) -> Result<Option<Bytes>, ProtocolError> {
        if self.listed_past(base) {
            return Ok(None);
        }
        self.read_version(base)
    }

    /// Whether one listing of `repo.d` shows a marker above `base`.
    fn listed_past(&self, base: &Version) -> bool {
        nudge(&self.dir());
        match self.list_markers() {
            Ok(Listed::Markers(markers)) => Self::highest(&markers)
                .is_some_and(|marker| marker.version.n() > base.n()),
            Ok(Listed::NotFound) => false,
            Err(err) => {
                tracing::debug!(?err, "could not list repo.d before a backup");
                false
            }
        }
    }

    /// `None` when the file is missing or unreadable (the base is gone).
    pub(crate) fn read_version(
        &self,
        version: &Version,
    ) -> Result<Option<Bytes>, ProtocolError> {
        match read_version_file(&self.dir(), version) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(ProtocolError::Io(err)) => {
                tracing::debug!(%err, "could not read a version of repo");
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }
}

/// `<N>.<h>`, checked against `h`.
pub(super) fn read_version_file(
    dir: &Path,
    version: &Version,
) -> Result<Bytes, ProtocolError> {
    let name = version.file_name();
    let bytes = read_all(&dir.join(&name))?;
    let read = Hash::of(&bytes);
    if read == *version.hash() {
        Ok(bytes)
    } else {
        Err(ProtocolError::Corrupt(format!(
            "{name} in {} doesn't hash to its name: read {} bytes hashing to {}",
            dir.display(),
            bytes.len(),
            read.as_str()
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        super::{NO_MARKER, testing::*},
        *,
    };
    use crate::hooks::NoHooks;

    fn repo(root: &Path) -> RepoD<'_> {
        RepoD { root, hooks: &NoHooks }
    }

    fn current(bytes: &[u8], version: &Version) -> Option<Current> {
        Some(Current { bytes: Bytes::copy_from_slice(bytes), version: version.clone() })
    }

    fn modified(bytes: &[u8], version: &Version) -> Option<Since> {
        current(bytes, version).map(Since::Modified)
    }

    #[test]
    fn reads_the_current_version() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        assert_eq!(repo(root.path()).read_latest().unwrap(), current(&v0_bytes(), &v0));
    }

    #[test]
    fn previous_or_newer_version_is_not_modified() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let repo = repo(root.path());
        assert_eq!(repo.read_since(&v0).unwrap(), Some(Since::NotModified));
        // A listing that lags behind what the caller already read.
        let v3 = version(3, &repo_info(3));
        assert_eq!(repo.read_since(&v3).unwrap(), Some(Since::NotModified));
        // Same N, other hash: a version this repo never had.
        let other = version(0, &repo_info(10));
        assert_eq!(repo.read_since(&other).unwrap(), modified(&v0_bytes(), &v0));
        let older = version(0, &repo_info(11));
        let v1 = version(1, &repo_info(1));
        let dir = root.path().join("repo.d");
        fs::write(dir.join(v1.file_name()), repo_info(1)).unwrap();
        fs::rename(dir.join(latest(&v0)), dir.join(latest(&v1))).unwrap();
        assert_eq!(repo.read_since(&older).unwrap(), modified(&repo_info(1), &v1));
    }

    #[test]
    fn missing_repo_d_is_not_found() {
        let root = tempfile::tempdir().unwrap();
        let repo = repo(root.path());
        assert_eq!(repo.read_latest().unwrap(), None);
        assert_eq!(repo.read_since(&version(0, &v0_bytes())).unwrap(), None);
    }

    #[test]
    fn empty_repo_d_has_no_consistent_view() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("repo.d")).unwrap();
        let err = repo(root.path()).read_latest().unwrap_err();
        assert!(
            matches!(&err, ProtocolError::NoConsistentView(cause) if cause == NO_MARKER),
            "{err:?}"
        );
    }

    #[test]
    fn wrong_bytes_are_never_returned() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        fs::write(root.path().join("repo.d").join(v0.file_name()), b"not v0").unwrap();
        let err = repo(root.path()).read_latest().unwrap_err();
        assert!(
            matches!(&err, ProtocolError::NoConsistentView(cause)
                if cause.contains(&v0.file_name()) && cause.contains("doesn't hash to its name")
                    && !cause.contains("is a copy")),
            "{err:?}"
        );
    }

    #[test]
    fn missing_version_file_has_no_consistent_view() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        fs::remove_file(root.path().join("repo.d").join(v0.file_name())).unwrap();
        let err = repo(root.path()).read_latest().unwrap_err();
        let copy = format!(
            "; if the repository at {} is a copy, it may not have been taken as \
             \"Copying a repository\" in the filesystem layout specification says",
            root.path().display()
        );
        assert!(
            matches!(&err, ProtocolError::NoConsistentView(cause)
                if cause.starts_with(&format!("could not read {}: ", v0.file_name()))
                    && cause.ends_with(&copy)),
            "{err:?}"
        );
    }

    #[test]
    fn missing_files_of_a_moving_marker_are_not_blamed_on_a_copy() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v1 = version(1, &repo_info(1));
        let dir = root.path().join("repo.d");
        fs::remove_file(dir.join(v0.file_name())).unwrap();
        let moved = std::cell::Cell::new(false);
        // Between the first listing and its read, a change moves the marker
        // to `v1`, whose file is gone too by the next read.
        let err = repo(root.path())
            .read_listed(
                |_| {
                    if !moved.replace(true) {
                        fs::rename(dir.join(latest(&v0)), dir.join(latest(&v1))).unwrap();
                    }
                    None::<()>
                },
                |_| (),
            )
            .unwrap_err();
        assert!(
            matches!(&err, ProtocolError::NoConsistentView(cause)
                if cause.starts_with(&format!("could not read {}: ", v1.file_name()))
                    && !cause.contains("is a copy")),
            "{err:?}"
        );
    }

    #[test]
    fn torn_listing_takes_the_highest_marker() {
        let root = tempfile::tempdir().unwrap();
        seed(root.path());
        let v1 = version(1, &repo_info(1));
        let dir = root.path().join("repo.d");
        fs::write(dir.join(v1.file_name()), repo_info(1)).unwrap();
        fs::write(dir.join(latest(&v1)), b"").unwrap();
        assert_eq!(repo(root.path()).read_latest().unwrap(), current(&repo_info(1), &v1));
    }

    #[test]
    fn other_names_are_ignored() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v9 = version(9, &repo_info(9));
        let dir = root.path().join("repo.d");
        for name in [
            ".nfs123".to_string(),
            "garbage".to_string(),
            format!("latest.{}", v9.file_name()),
            format!("latest.000000000000000.{}", v9.file_name()),
            format!("{}.x", latest(&v9)),
            v9.file_name(),
        ] {
            fs::write(dir.join(name), repo_info(9)).unwrap();
        }
        assert_eq!(repo(root.path()).read_latest().unwrap(), current(&v0_bytes(), &v0));
    }

    /// Only a listed marker above the version stops the backup.
    #[test]
    fn read_backup_source_needs_a_marker_above_to_refuse() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let repo = repo(root.path());
        let dir = root.path().join("repo.d");
        let v0_read = Some(Bytes::from(v0_bytes()));
        assert_eq!(repo.read_backup_source(&v0).unwrap(), v0_read);

        let v1 = version(1, &repo_info(1));
        fs::write(dir.join(v1.file_name()), repo_info(1)).unwrap();
        fs::rename(dir.join(latest(&v0)), dir.join(latest(&v1))).unwrap();
        assert_eq!(repo.read_backup_source(&v0).unwrap(), None);
        assert_eq!(repo.read_version(&v0).unwrap(), v0_read);

        // A listing that lags behind the version, or shows no marker.
        fs::rename(dir.join(latest(&v1)), dir.join(latest(&v0))).unwrap();
        let v1_read = Some(Bytes::from(repo_info(1)));
        assert_eq!(repo.read_backup_source(&v1).unwrap(), v1_read);
        fs::remove_file(dir.join(latest(&v0))).unwrap();
        assert_eq!(repo.read_backup_source(&v1).unwrap(), v1_read);
    }

    #[test]
    fn read_version_reads_any_version_file() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let repo = repo(root.path());
        assert_eq!(repo.read_version(&v0).unwrap(), Some(Bytes::from(v0_bytes())));
        assert_eq!(repo.read_version(&version(1, &repo_info(1))).unwrap(), None);

        let v2 = version(2, &repo_info(2));
        fs::write(root.path().join("repo.d").join(v2.file_name()), b"not v2").unwrap();
        let err = repo.read_version(&v2).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Corrupt(msg) if msg.contains(&v2.file_name())),
            "{err:?}"
        );
    }
}
