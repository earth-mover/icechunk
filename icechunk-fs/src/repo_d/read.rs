//! Reads of `repo`: `read_latest` and the version files themselves.

use std::path::Path;

use bytes::Bytes;

use super::{ProtocolError, RepoD, Step};
use crate::{
    names::{Hash, Version},
    read::read_all,
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
                    Step::Retry(match err {
                        ProtocolError::Corrupt(msg) => msg,
                        ProtocolError::Io(err) => {
                            format!("could not read {}: {err}", version.file_name())
                        }
                        err => format!("{err:?}"),
                    })
                }
            })
        })
    }

    /// For `copy_object`: `None` when the file is missing or unreadable (the
    /// base is gone).
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
fn read_version_file(dir: &Path, version: &Version) -> Result<Bytes, ProtocolError> {
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
        assert_eq!(repo(root.path()).read_latest().unwrap(), current(V0, &v0));
    }

    #[test]
    fn previous_or_newer_version_is_not_modified() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let repo = repo(root.path());
        assert_eq!(repo.read_since(&v0).unwrap(), Some(Since::NotModified));
        // A listing that lags behind what the caller already read.
        let v3 = version(3, b"v3");
        assert_eq!(repo.read_since(&v3).unwrap(), Some(Since::NotModified));
        // Same N, other hash: a version this repo never had.
        let other = version(0, b"other");
        assert_eq!(repo.read_since(&other).unwrap(), modified(V0, &v0));
        let older = version(0, b"older");
        let v1 = version(1, b"v1");
        let dir = root.path().join("repo.d");
        fs::write(dir.join(v1.file_name()), b"v1").unwrap();
        fs::rename(dir.join(latest(&v0)), dir.join(latest(&v1))).unwrap();
        assert_eq!(repo.read_since(&older).unwrap(), modified(b"v1", &v1));
    }

    #[test]
    fn missing_repo_d_is_not_found() {
        let root = tempfile::tempdir().unwrap();
        let repo = repo(root.path());
        assert_eq!(repo.read_latest().unwrap(), None);
        assert_eq!(repo.read_since(&version(0, V0)).unwrap(), None);
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
                if cause.contains(&v0.file_name()) && cause.contains("doesn't hash to its name")),
            "{err:?}"
        );
    }

    #[test]
    fn missing_version_file_has_no_consistent_view() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        fs::remove_file(root.path().join("repo.d").join(v0.file_name())).unwrap();
        let err = repo(root.path()).read_latest().unwrap_err();
        assert!(
            matches!(&err, ProtocolError::NoConsistentView(cause)
                if cause.starts_with(&format!("could not read {}: ", v0.file_name()))),
            "{err:?}"
        );
    }

    #[test]
    fn torn_listing_takes_the_highest_marker() {
        let root = tempfile::tempdir().unwrap();
        seed(root.path());
        let v1 = version(1, b"v1");
        let dir = root.path().join("repo.d");
        fs::write(dir.join(v1.file_name()), b"v1").unwrap();
        fs::write(dir.join(latest(&v1)), b"").unwrap();
        assert_eq!(repo(root.path()).read_latest().unwrap(), current(b"v1", &v1));
    }

    #[test]
    fn other_names_are_ignored() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v9 = version(9, b"v9");
        let dir = root.path().join("repo.d");
        for name in [
            ".nfs123".to_string(),
            "garbage".to_string(),
            format!("latest.{}", v9.file_name()),
            format!("latest.000000000000000.{}", v9.file_name()),
            format!("{}.x", latest(&v9)),
            v9.file_name(),
        ] {
            fs::write(dir.join(name), b"v9").unwrap();
        }
        assert_eq!(repo(root.path()).read_latest().unwrap(), current(V0, &v0));
    }

    #[test]
    fn frozen_marker_is_read() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = root.path().join("repo.d");
        fs::rename(dir.join(latest(&v0)), dir.join(frozen(&v0))).unwrap();
        assert_eq!(repo(root.path()).read_latest().unwrap(), current(V0, &v0));
    }

    #[test]
    fn read_version_reads_any_version_file() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let repo = repo(root.path());
        assert_eq!(repo.read_version(&v0).unwrap(), Some(Bytes::from_static(V0)));
        assert_eq!(repo.read_version(&version(1, b"v1")).unwrap(), None);

        let v2 = version(2, b"v2");
        fs::write(root.path().join("repo.d").join(v2.file_name()), b"not v2").unwrap();
        let err = repo.read_version(&v2).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Corrupt(msg) if msg.contains(&v2.file_name())),
            "{err:?}"
        );
    }
}
