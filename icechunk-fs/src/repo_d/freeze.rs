//! Freezing: the marker is renamed to `frozen.*`, a name no commit renames
//! from, and to a new `latest.*` to unfreeze.

use std::{fs, io};

use super::{ProtocolError, RepoD, Step};
use crate::{
    names::{Marker, MarkerKind, Version},
    write::fsync_dir,
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FreezeResult {
    Frozen(Version),
    AlreadyFrozen,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UnfreezeResult {
    Unfrozen(Version),
    NotFrozen,
}

impl RepoD<'_> {
    /// Renames `latest.<N>.<h>` to `frozen.<N>.<h>`: version `N` stays
    /// readable and no commit can succeed.
    pub(crate) fn freeze(&self) -> Result<FreezeResult, ProtocolError> {
        self.with_marker(|marker| match marker.kind {
            MarkerKind::Frozen => Ok(Step::Done(FreezeResult::AlreadyFrozen)),
            MarkerKind::Latest => {
                let frozen =
                    Marker { kind: MarkerKind::Frozen, version: marker.version.clone() };
                Ok(self
                    .move_marker(marker, &frozen)?
                    .map(|()| FreezeResult::Frozen(marker.version.clone())))
            }
        })
    }

    /// Links `<N>.<h>` as `<N+1>.<h>` and renames `frozen.<N>.<h>` to
    /// `latest.<N+1>.<h>`, so that no marker name is ever used twice. Writers
    /// based on `(N, h)` get one conflict.
    pub(crate) fn unfreeze(&self) -> Result<UnfreezeResult, ProtocolError> {
        let dir = self.dir();
        self.with_marker(|marker| match marker.kind {
            MarkerKind::Latest => Ok(Step::Done(UnfreezeResult::NotFrozen)),
            MarkerKind::Frozen => {
                let current = &marker.version;
                let next = current.next(current.hash().clone())?;
                match fs::hard_link(
                    dir.join(current.file_name()),
                    dir.join(next.file_name()),
                ) {
                    Ok(()) => {}
                    // The name is the hash, so whatever holds it has the bytes
                    // of `h`: an earlier unfreeze's link, or that of a writer
                    // based on `(N, h)` committing the same bytes.
                    Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
                    // A commit after another unfreeze deleted it.
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        return Ok(Step::Retry(format!(
                            "could not link {}: {err}",
                            current.file_name()
                        )));
                    }
                    Err(err) => return Err(err.into()),
                }
                fsync_dir(&dir)?;
                let latest = Marker { kind: MarkerKind::Latest, version: next.clone() };
                Ok(self
                    .move_marker(marker, &latest)?
                    .map(|()| UnfreezeResult::Unfrozen(next)))
            }
        })
    }

    /// Renames the marker `from` to `to` and makes it durable. A retry if
    /// `from` is gone: a commit, freeze or unfreeze got in first.
    fn move_marker(&self, from: &Marker, to: &Marker) -> io::Result<Step<()>> {
        let dir = self.dir();
        match fs::rename(dir.join(from.file_name()), dir.join(to.file_name())) {
            Ok(()) => fsync_dir(&dir).map(Step::Done),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Ok(Step::Retry(format!("{} is gone: {err}", from.file_name())))
            }
            Err(err) => Err(err),
        }
    }

    /// [`RepoD::with_listing`], where a missing `repo.d` is an error, naming
    /// the remedy if the `open` classification has one.
    fn with_marker<T>(
        &self,
        mut step: impl FnMut(&Marker) -> Result<Step<T>, ProtocolError>,
    ) -> Result<T, ProtocolError> {
        self.with_listing(|marker| match marker {
            Some(marker) => step(marker),
            None if self.opens()? => {
                Ok(Step::Retry(format!("{} appeared", self.dir().display())))
            }
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no repository: {} doesn't exist", self.dir().display()),
            )
            .into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use bytes::Bytes;

    use super::{super::testing::*, FreezeResult::*, UnfreezeResult::*, *};
    use crate::{
        hooks::{NoHooks, Point},
        repo_d::{NO_MARKER, commit::UpdateOutcome, read::Current},
    };

    const V1: &[u8] = b"v1";

    fn repo(root: &Path) -> RepoD<'_> {
        RepoD { root, hooks: &NoHooks }
    }

    #[test]
    fn freeze_keeps_the_version_readable_and_refuses_commits() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let repo = repo(root.path());
        let dir = root.path().join("repo.d");
        assert_eq!(repo.freeze().unwrap(), Frozen(v0.clone()));
        assert_eq!(names(&dir), set([v0.file_name(), frozen(&v0)]));
        assert_eq!(
            repo.read_latest().unwrap(),
            Some(Current { bytes: Bytes::from_static(V0), version: v0.clone() })
        );
        let err = repo.conditional_update(V1, &v0).unwrap_err();
        assert!(matches!(err, ProtocolError::Frozen), "{err:?}");
        assert_eq!(repo.freeze().unwrap(), AlreadyFrozen);
        assert!(dir.join(frozen(&v0)).exists());
    }

    #[test]
    fn unfreeze_moves_the_marker_to_a_new_version() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let repo = repo(root.path());
        repo.freeze().unwrap();
        let v1 = version(1, V0);
        assert_eq!(repo.unfreeze().unwrap(), Unfrozen(v1.clone()));
        let dir = root.path().join("repo.d");
        assert_eq!(names(&dir), set([v0.file_name(), v1.file_name(), latest(&v1)]));
        assert_eq!(fs::read(dir.join(v1.file_name())).unwrap(), V0);
        assert_eq!(repo.unfreeze().unwrap(), NotFrozen);

        // A writer based on the frozen version gets one conflict.
        assert_eq!(
            repo.conditional_update(V1, &v0).unwrap(),
            UpdateOutcome::NotOnLatestVersion
        );
        let v2 = version(2, V1);
        assert_eq!(repo.conditional_update(V1, &v1).unwrap(), UpdateOutcome::Updated(v2));
    }

    #[test]
    fn unfreeze_of_a_repo_not_frozen_changes_nothing() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        assert_eq!(repo(root.path()).unfreeze().unwrap(), NotFrozen);
        assert_eq!(
            names(&root.path().join("repo.d")),
            set([v0.file_name(), latest(&v0)])
        );
    }

    #[test]
    fn unfreeze_resumes_after_the_link() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let repo = repo(root.path());
        repo.freeze().unwrap();
        let dir = root.path().join("repo.d");
        let v1 = version(1, V0);
        fs::hard_link(dir.join(v0.file_name()), dir.join(v1.file_name())).unwrap();
        assert_eq!(repo.unfreeze().unwrap(), Unfrozen(v1.clone()));
        assert_eq!(names(&dir), set([v0.file_name(), v1.file_name(), latest(&v1)]));
    }

    #[test]
    fn freeze_takes_the_marker_from_under_a_commit() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let (result, _) = with_paused(
            TestHooks::default(),
            Point::BeforeRename,
            |hooks| RepoD { root: root.path(), hooks }.conditional_update(V1, &v0),
            || assert_eq!(repo(root.path()).freeze().unwrap(), Frozen(v0.clone())),
        );
        assert!(matches!(result, Err(ProtocolError::Frozen)), "{result:?}");
        assert!(root.path().join("repo.d").join(frozen(&v0)).exists());
    }

    #[test]
    fn freeze_without_a_repository_is_an_error() {
        let root = tempfile::tempdir().unwrap();
        let repo = repo(root.path());
        for err in [repo.freeze().unwrap_err(), repo.unfreeze().unwrap_err()] {
            assert!(
                matches!(&err, ProtocolError::Io(err) if err.kind() == io::ErrorKind::NotFound),
                "{err:?}"
            );
        }
        assert!(names(root.path()).is_empty());
    }

    #[test]
    fn freeze_without_a_marker_gives_up() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("repo.d")).unwrap();
        let repo = repo(root.path());
        for err in [repo.freeze().unwrap_err(), repo.unfreeze().unwrap_err()] {
            assert!(
                matches!(&err, ProtocolError::NoConsistentView(cause) if cause == NO_MARKER),
                "{err:?}"
            );
        }
    }

    #[test]
    fn unfreeze_without_the_frozen_version_gives_up() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let repo = repo(root.path());
        repo.freeze().unwrap();
        let dir = root.path().join("repo.d");
        fs::remove_file(dir.join(v0.file_name())).unwrap();
        let err = repo.unfreeze().unwrap_err();
        assert!(
            matches!(&err, ProtocolError::NoConsistentView(cause) if cause.contains(&v0.file_name())),
            "{err:?}"
        );
        assert_eq!(names(&dir), set([frozen(&v0)]));
    }
}
