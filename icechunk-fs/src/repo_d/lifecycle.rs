//! Creating and opening a repository in the filesystem layout, see
//! "Creation" and "Opening" in design-docs/020-filesystem-storage.md.

use std::{
    fs, io,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use icechunk_format::{layout_v1, writer_features::WriterFeature};
use uuid::Uuid;

use super::{ProtocolError, RepoD, Step, discard_tmp_dir, with_root_lock};
use crate::{
    hooks::{CreationStep, Point},
    names::{Hash, Marker, REPO, REPO_D, Version, tmp_dir_name},
    nudge::nudge,
    read::read_all,
    storage::is_missing,
    write::{fsync_dir, write_fresh},
};

/// The writer features `FilesystemStorage` implements, all of them required.
pub(crate) const FEATURES: [WriterFeature; 1] = [WriterFeature::FilesystemLayoutV1];

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CreateOutcome {
    Created(Version),
    AlreadyExists,
}

/// The classification of the root, see "Opening".
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpenStatus {
    /// `repo.d` exists.
    Ok,
    /// Neither `repo.d` nor `$ROOT/repo` exists.
    NotFound,
    /// `$ROOT/repo` holds a repo info and `repo.d` doesn't exist: a
    /// repository in the object store layout.
    ObjectStoreLayout,
    /// `$ROOT/repo` can't be read or decoded; why.
    Unreadable(String),
}

/// What `$ROOT/repo` holds, read while `repo.d` doesn't exist.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum RootRepo {
    Missing,
    /// A repo info, with these bytes: a repository in the object store
    /// layout. One that lists the feature is no exception: converting it
    /// fails.
    ObjectStore(Bytes),
    /// Not a regular file, or bytes that don't decode; why.
    Undecodable(String),
}

/// How a creation's directory rename ended.
#[derive(Debug, PartialEq, Eq)]
enum DirRename {
    Ours,
    /// Another creation won.
    Lost,
}

impl RepoD<'_> {
    /// Creates the repository with `bytes`, which must list the feature, as
    /// its first version: its `N` is the Unix time in milliseconds. It builds
    /// `repo.d.tmp.<id>` and renames it to `repo.d`: that rename creates the
    /// repository. A repository in the object store layout is an error; any
    /// other `$ROOT/repo` means the repository exists. Nothing is ever
    /// overwritten.
    pub(crate) fn create(&self, bytes: &[u8]) -> Result<CreateOutcome, ProtocolError> {
        check_in_layout(bytes)?;
        fs::create_dir_all(self.root)?;
        nudge(self.root);
        if self.repo_d_exists()? || self.root_repo_exists()? {
            return Ok(CreateOutcome::AlreadyExists);
        }

        let initial = Version::initial(unix_millis()?, Hash::of(bytes))?;
        let tmp = self.root.join(tmp_dir_name(Uuid::new_v4()));
        fs::DirBuilder::new().create(&tmp)?;
        let built = write_fresh(&tmp.join(initial.file_name()), bytes, true)
            .and_then(|()| write_marker(&tmp, &initial));
        if let Err(err) = built {
            self.discard(&tmp);
            return Err(err.into());
        }
        self.hooks.at(Point::Creation(CreationStep::Built));

        let renamed = self.hooks.rename(&tmp, &self.dir());
        match self.after_dir_rename(renamed, &tmp, &initial)? {
            DirRename::Ours => {
                self.hooks.at(Point::Creation(CreationStep::DirRenamed));
                Ok(CreateOutcome::Created(initial))
            }
            DirRename::Lost => Ok(CreateOutcome::AlreadyExists),
        }
    }

    /// The `open` classification. With `repo.d`, `$ROOT/repo` isn't read.
    pub(crate) fn open_status(&self) -> Result<OpenStatus, ProtocolError> {
        nudge(self.root);
        if self.repo_d_exists()? {
            return Ok(OpenStatus::Ok);
        }
        Ok(match self.root_repo() {
            Ok(RootRepo::Missing) => OpenStatus::NotFound,
            Ok(RootRepo::ObjectStore(_)) => OpenStatus::ObjectStoreLayout,
            Ok(RootRepo::Undecodable(why)) => OpenStatus::Unreadable(why),
            Err(err) => OpenStatus::Unreadable(err.to_string()),
        })
    }

    /// Whether the repository exists, by the `open` classification. The
    /// states that need a remedy are errors naming it.
    pub(crate) fn opens(&self) -> Result<bool, ProtocolError> {
        match self.open_status()? {
            OpenStatus::Ok => Ok(true),
            OpenStatus::NotFound => Ok(false),
            OpenStatus::ObjectStoreLayout => {
                Err(ProtocolError::object_store_layout(self.root))
            }
            OpenStatus::Unreadable(why) => {
                Err(ProtocolError::repo_unreadable(self.root, &why))
            }
        }
    }

    /// Step 2 of a creation, without `repo.d`: whether `$ROOT/repo` exists. A
    /// repository in the object store layout is an error naming the
    /// conversion.
    fn root_repo_exists(&self) -> Result<bool, ProtocolError> {
        match fs::symlink_metadata(self.root.join(REPO)) {
            Ok(_) => {}
            Err(err) if is_missing(&err) => return Ok(false),
            Err(err) => return Err(err.into()),
        }
        match self.root_repo() {
            Ok(RootRepo::ObjectStore(_)) => {
                Err(ProtocolError::object_store_layout(self.root))
            }
            // `Missing`: it vanished since, so something else is writing the root.
            Ok(RootRepo::Missing | RootRepo::Undecodable(_)) | Err(_) => Ok(true),
        }
    }

    /// Classifies `$ROOT/repo`; an error only if it can't be read.
    pub(super) fn root_repo(&self) -> io::Result<RootRepo> {
        let path = self.root.join(REPO);
        let bytes = match with_root_lock(self.root, || read_all(&path)) {
            Ok(bytes) => bytes,
            Err(err) if is_missing(&err) => return Ok(RootRepo::Missing),
            Err(_) if self.repo_is_not_a_file() => {
                return Ok(RootRepo::Undecodable("it isn't a regular file".to_string()));
            }
            Err(err) => return Err(err),
        };
        Ok(match layout_v1::lists_feature(&bytes) {
            Ok(_) => RootRepo::ObjectStore(bytes),
            Err(err) => RootRepo::Undecodable(format!("it doesn't decode: {err}")),
        })
    }

    /// The last step of a creation, given the result of the directory rename.
    fn after_dir_rename(
        &self,
        renamed: io::Result<()>,
        tmp: &Path,
        initial: &Version,
    ) -> Result<DirRename, ProtocolError> {
        match renamed {
            Ok(()) => {
                fsync_dir(self.root)?;
                Ok(DirRename::Ours)
            }
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty
                ) =>
            {
                self.discard(tmp);
                Ok(DirRename::Lost)
            }
            // Our rename may have landed and its reply been lost: `tmp` is gone.
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                match self.listed_marker()? {
                    Some(marker) if marker.version == *initial => {
                        fsync_dir(self.root)?;
                        Ok(DirRename::Ours)
                    }
                    Some(_) => Ok(DirRename::Lost),
                    None => Err(io::Error::other(format!(
                        "renaming {} to repo.d failed and repo.d doesn't exist: retry",
                        tmp.display()
                    ))
                    .into()),
                }
            }
            Err(err) => {
                self.discard(tmp);
                Err(err.into())
            }
        }
    }

    /// `repo` exists and is not a regular file. A symlink isn't followed.
    pub(super) fn repo_is_not_a_file(&self) -> bool {
        fs::symlink_metadata(self.root.join(REPO))
            .is_ok_and(|metadata| !metadata.is_file())
    }

    /// Whether a listing of the root shows `repo.d`.
    pub(super) fn repo_d_exists(&self) -> io::Result<bool> {
        let entries = match fs::read_dir(self.root) {
            Err(err) if is_missing(&err) => return Ok(false),
            entries => entries?,
        };
        for entry in entries {
            if entry?.file_name() == REPO_D {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Liveness only: the sweep deletes a creation directory left behind.
    pub(super) fn discard(&self, tmp: &Path) {
        if let Err(err) = discard_tmp_dir(self.root, tmp) {
            tracing::debug!(%err, path = %tmp.display(), "could not delete a creation directory");
        }
    }

    /// The marker of a listing of `repo.d`, listing again while it shows
    /// none. `None` if `repo.d` doesn't exist.
    fn listed_marker(&self) -> Result<Option<Marker>, ProtocolError> {
        self.with_listing(|marker| Ok(Step::Done(marker.cloned())))
    }
}

/// Step 1 of a creation: `bytes` list the feature.
pub(crate) fn check_in_layout(bytes: &[u8]) -> Result<(), ProtocolError> {
    match layout_v1::lists_feature(bytes) {
        Ok(true) => Ok(()),
        Ok(false) => Err(ProtocolError::Other(format!(
            "the repo info of a new repository must list {}",
            layout_v1::FEATURE
        ))),
        Err(err) => Err(ProtocolError::Other(format!(
            "the repo info of a new repository must list {} and doesn't decode: {err}",
            layout_v1::FEATURE
        ))),
    }
}

/// The marker of `initial` in the directory `tmp`, durably.
pub(super) fn write_marker(tmp: &Path, initial: &Version) -> io::Result<()> {
    let marker = Marker { version: initial.clone() };
    write_fresh(&tmp.join(marker.file_name()), &[], true)?;
    fsync_dir(tmp)
}

/// The first `N` of a new `repo.d`.
pub(super) fn unix_millis() -> io::Result<u64> {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| io::Error::other(format!("the clock is before 1970: {err}")))?;
    u64::try_from(since_epoch.as_millis())
        .map_err(|err| io::Error::other(format!("the clock is out of range: {err}")))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Barrier,
        thread,
        time::{Duration, SystemTime},
    };

    use uuid::Uuid;

    use super::{super::testing::*, CreateOutcome::*, *};
    use crate::{
        hooks::{CreationStep, NoHooks, Point},
        names::tmp_dir_name,
        path::staging_path,
        repo_d::{commit::UpdateOutcome, read::Current},
    };

    fn repo(root: &Path) -> RepoD<'_> {
        RepoD { root, hooks: &NoHooks }
    }

    /// Exactly `repo.d`, holding `v0` with `bytes` and its marker.
    fn assert_created(root: &Path, v0: &Version, bytes: &[u8]) {
        assert_eq!(names(root), strings(["repo.d"]));
        let dir = root.join("repo.d");
        assert_eq!(names(&dir), set([v0.file_name(), latest(v0)]));
        assert_eq!(fs::read(dir.join(v0.file_name())).unwrap(), bytes);
        assert_eq!(fs::read(dir.join(latest(v0))).unwrap(), b"");
    }

    // ── create ──────────────────────────────────────────────────────────────

    #[test]
    fn create_in_an_empty_root() {
        let root = tempfile::tempdir().unwrap();
        let v0 = create(root.path(), &NoHooks, &v0_bytes());
        assert_created(root.path(), &v0, &v0_bytes());
    }

    #[test]
    fn create_makes_a_missing_root() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("a/b");
        let v0 = create(&missing, &NoHooks, &v0_bytes());
        assert_created(&missing, &v0, &v0_bytes());
    }

    #[test]
    fn create_reaches_every_creation_point() {
        let root = tempfile::tempdir().unwrap();
        let hooks = TestHooks::default();
        create(root.path(), &hooks, &v0_bytes());
        assert_eq!(
            hooks.reached(),
            [
                Point::Creation(CreationStep::Built),
                Point::Creation(CreationStep::DirRenamed),
            ]
        );
    }

    #[test]
    fn create_again_exists_and_changes_nothing() {
        let root = tempfile::tempdir().unwrap();
        let v0 = create(root.path(), &NoHooks, &v0_bytes());
        for bytes in [v0_bytes(), repo_info(2)] {
            assert_eq!(repo(root.path()).create(&bytes).unwrap(), AlreadyExists);
            assert_created(root.path(), &v0, &v0_bytes());
        }
    }

    #[test]
    fn create_after_a_commit_exists() {
        let root = tempfile::tempdir().unwrap();
        let repo = repo(root.path());
        let v0 = create(root.path(), &NoHooks, &v0_bytes());
        assert!(matches!(
            repo.conditional_update(&repo_info(1), &v0).unwrap(),
            UpdateOutcome::Updated(_)
        ));
        assert_eq!(repo.create(&v0_bytes()).unwrap(), AlreadyExists);
    }

    #[test]
    fn create_next_to_repo_d_exists() {
        let root = tempfile::tempdir().unwrap();
        repo_d_with(root.path(), &repo_info(3));
        assert_eq!(repo(root.path()).create(&v0_bytes()).unwrap(), AlreadyExists);
        assert_eq!(names(root.path()), strings(["repo.d"]));
    }

    #[test]
    fn create_refuses_bytes_outside_the_layout() {
        for bytes in [featureless_repo_info(0), UNDECODABLE.to_vec()] {
            let root = tempfile::tempdir().unwrap();
            let err = repo(root.path()).create(&bytes).unwrap_err();
            assert!(matches!(&err, ProtocolError::Other(_)), "{err:?}");
            assert!(names(root.path()).is_empty());
        }
    }

    /// Any repo info, with the feature or without.
    #[test]
    fn create_over_a_fresh_object_store_repo_is_an_error() {
        for planted in [featureless_repo_info(3), repo_info(3)] {
            let root = tempfile::tempdir().unwrap();
            plant(root.path(), &planted);
            let err = repo(root.path()).create(&v0_bytes()).unwrap_err();
            assert!(
                matches!(&err, ProtocolError::Other(msg) if msg.contains("is in the object store layout")),
                "{err:?}"
            );
            assert_eq!(names(root.path()), strings(["repo"]));
            assert_eq!(fs::read(root.path().join("repo")).unwrap(), planted);
        }
    }

    #[test]
    fn create_over_a_repo_file_exists() {
        for planted in [UNDECODABLE.to_vec(), Vec::new()] {
            let root = tempfile::tempdir().unwrap();
            plant(root.path(), &planted);
            assert_eq!(repo(root.path()).create(&v0_bytes()).unwrap(), AlreadyExists);
            assert_eq!(names(root.path()), strings(["repo"]));
            assert_eq!(fs::read(root.path().join("repo")).unwrap(), planted);
        }
    }

    #[test]
    fn create_over_a_repo_that_is_not_a_file_exists() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("repo")).unwrap();
        assert_eq!(repo(root.path()).create(&v0_bytes()).unwrap(), AlreadyExists);
        assert_eq!(names(root.path()), strings(["repo"]));
    }

    #[test]
    fn create_over_an_unreadable_repo_file_exists() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("repo");
        fs::write(&path, repo_info(3)).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
        // Root reads it anyway: nothing to test.
        if fs::read(&path).is_ok() {
            return;
        }
        assert_eq!(repo(root.path()).create(&v0_bytes()).unwrap(), AlreadyExists);
        assert_eq!(names(root.path()), strings(["repo"]));
    }

    #[test]
    fn concurrent_creates_one_wins_and_repo_is_the_winners() {
        for _ in 0..20 {
            let root = tempfile::tempdir().unwrap();
            let repo = repo(root.path());
            let barrier = Barrier::new(2);
            let create = |bytes: Vec<u8>| {
                barrier.wait();
                let outcome = repo.create(&bytes).unwrap();
                (bytes, outcome)
            };
            let results = thread::scope(|scope| {
                let a = scope.spawn(|| create(repo_info(10)));
                let b = scope.spawn(|| create(repo_info(11)));
                [a.join().unwrap(), b.join().unwrap()]
            });
            let winners: Vec<_> = results
                .iter()
                .filter_map(|(bytes, outcome)| match outcome {
                    Created(v) => Some((bytes, v.clone())),
                    AlreadyExists => None,
                })
                .collect();
            let [(bytes, v0)] = winners.as_slice() else {
                panic!("exactly one create wins: {results:?}")
            };
            assert_eq!(
                repo.read_latest().unwrap(),
                Some(Current {
                    bytes: Bytes::copy_from_slice(bytes),
                    version: v0.clone()
                })
            );
            assert_created(root.path(), v0, bytes);
        }
    }

    #[test]
    fn a_lost_reply_of_the_directory_rename_is_ours() {
        let root = tempfile::tempdir().unwrap();
        let v0 = create(root.path(), &lose_rename_reply_onto("repo.d"), &v0_bytes());
        assert_created(root.path(), &v0, &v0_bytes());
    }

    #[test]
    fn a_directory_rename_that_did_not_land_is_an_error() {
        let root = tempfile::tempdir().unwrap();
        // The tmp directory is gone and no `repo.d` exists.
        let hooks = TestHooks::default().rename(|from, to| {
            if to.file_name().is_some_and(|n| n == "repo.d") {
                fs::remove_dir_all(from)?;
                Err(io::ErrorKind::NotFound.into())
            } else {
                fs::rename(from, to)
            }
        });
        let err =
            RepoD { root: root.path(), hooks: &hooks }.create(&v0_bytes()).unwrap_err();
        assert!(matches!(&err, ProtocolError::Io(_)), "{err:?}");
        assert!(names(root.path()).is_empty());
        assert_eq!(repo(root.path()).open_status().unwrap(), OpenStatus::NotFound);
    }

    /// The directory rename reports `ENOENT` and the listing shows another
    /// creator's marker: it exists.
    #[test]
    fn a_lost_directory_rename_reply_with_another_marker_exists() {
        let root = tempfile::tempdir().unwrap();
        let winner_root = root.path().to_path_buf();
        let hooks = TestHooks::default().rename(move |from, to| {
            if to.file_name().is_some_and(|n| n == "repo.d") {
                fs::remove_dir_all(from)?;
                repo_d_with(&winner_root, &repo_info(11));
                Err(io::ErrorKind::NotFound.into())
            } else {
                fs::rename(from, to)
            }
        });
        let outcome =
            RepoD { root: root.path(), hooks: &hooks }.create(&v0_bytes()).unwrap();
        assert_eq!(outcome, AlreadyExists);
        let winner = version(0, &repo_info(11));
        assert_eq!(names(root.path()), strings(["repo.d"]));
        assert_eq!(
            names(&root.path().join("repo.d")),
            set([winner.file_name(), latest(&winner)])
        );
    }

    #[test]
    fn a_leftover_repo_staging_is_swept_by_age() {
        let root = tempfile::tempdir().unwrap();
        let staging = staging_path(&root.path().join("repo"));
        fs::write(&staging, repo_info(4)).unwrap();
        assert_eq!(repo(root.path()).open_status().unwrap(), OpenStatus::NotFound);
        let v0 = create(root.path(), &NoHooks, &v0_bytes());
        assert!(staging.exists());
        let young = SystemTime::now() - Duration::from_secs(3600);
        super::super::sweep::sweep(root.path(), young, false, &NoHooks).unwrap();
        assert!(staging.exists());
        fs::File::open(&staging)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(7200))
            .unwrap();
        super::super::sweep::sweep(root.path(), young, false, &NoHooks).unwrap();
        assert_created(root.path(), &v0, &v0_bytes());
    }

    // ── open_status ─────────────────────────────────────────────────────────

    #[test]
    fn open_status_classifies_every_row() {
        let status = |setup: &dyn Fn(&Path)| {
            let root = tempfile::tempdir().unwrap();
            setup(root.path());
            repo(root.path()).open_status().unwrap()
        };
        // `repo.d`, whatever `$ROOT/repo` holds: it isn't read.
        assert_eq!(
            status(&|root| {
                create(root, &NoHooks, &v0_bytes());
            }),
            OpenStatus::Ok
        );
        for planted in [
            None,
            Some(featureless_repo_info(1)),
            Some(repo_info(1)),
            Some(UNDECODABLE.to_vec()),
        ] {
            assert_eq!(
                status(&|root| {
                    repo_d_with(root, &v0_bytes());
                    if let Some(bytes) = &planted {
                        plant(root, bytes);
                    }
                }),
                OpenStatus::Ok,
                "{planted:?}"
            );
        }
        assert_eq!(
            status(&|root| {
                repo_d_with(root, &v0_bytes());
                fs::create_dir_all(root.join("repo")).unwrap();
            }),
            OpenStatus::Ok
        );
        // Any repo info, with the feature or without.
        for planted in [featureless_repo_info(1), repo_info(1)] {
            assert_eq!(
                status(&|root| plant(root, &planted)),
                OpenStatus::ObjectStoreLayout
            );
        }
        for planted in [UNDECODABLE.to_vec(), Vec::new()] {
            assert!(
                matches!(
                    status(&|root| plant(root, &planted)),
                    OpenStatus::Unreadable(_)
                ),
                "{planted:?}"
            );
        }
        assert!(matches!(
            status(&|root| fs::create_dir_all(root.join("repo")).unwrap()),
            OpenStatus::Unreadable(_)
        ));
        assert_eq!(status(&|_| {}), OpenStatus::NotFound);
        // Other names alone are no repository.
        assert_eq!(
            status(&|root| {
                fs::create_dir_all(root.join(tmp_dir_name(Uuid::new_v4()))).unwrap();
                fs::write(staging_path(&root.join("repo")), v0_bytes()).unwrap();
                // Unknown names.
                fs::write(root.join(format!("repo.legacy.{}", Uuid::new_v4())), b"")
                    .unwrap();
                fs::write(root.join(format!("repo.stub.{}", Uuid::new_v4())), b"")
                    .unwrap();
            }),
            OpenStatus::NotFound
        );

        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("missing");
        assert_eq!(repo(&missing).open_status().unwrap(), OpenStatus::NotFound);
    }

    #[test]
    fn opens_refuses_the_error_rows_naming_what_it_found() {
        let opens = |planted: &[u8]| {
            let root = tempfile::tempdir().unwrap();
            plant(root.path(), planted);
            let opened = repo(root.path()).opens();
            assert_eq!(fs::read(root.path().join("repo")).unwrap(), planted);
            opened
        };
        for planted in [featureless_repo_info(1), repo_info(1)] {
            let err = opens(&planted).unwrap_err();
            assert!(
                matches!(&err, ProtocolError::Other(msg) if msg.contains("is in the object store layout")),
                "{err:?}"
            );
        }
        let err = opens(UNDECODABLE).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("can't be read or decoded and")),
            "{err:?}"
        );
        let root = tempfile::tempdir().unwrap();
        assert!(!repo(root.path()).opens().unwrap());
    }
}
