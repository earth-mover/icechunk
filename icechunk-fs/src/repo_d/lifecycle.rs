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

use super::{
    ProtocolError, RepoD, Step, discard_tmp_dir,
    mirror::{PublishOutcome, with_root_lock},
};
use crate::{
    hooks::{CreationStep, Point},
    names::{Hash, Marker, REPO, REPO_D, Version, tmp_dir_name},
    nudge::nudge,
    path::staging_path,
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
    /// Nothing, or an interrupted creation.
    NotFound,
    /// `$ROOT/repo` lists the feature with a longer history, without `repo.d`.
    OrphanMirror,
    /// `$ROOT/repo` doesn't list the feature: a repository in the standard
    /// layout.
    StandardLayout,
    /// `$ROOT/repo` can't be read or decoded; why.
    Unreadable(String),
}

/// What `$ROOT/repo` holds, read while `repo.d` doesn't exist.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum RootRepo {
    Missing,
    /// It lists the feature, and its ops log is only `RepoInitializedUpdate`.
    InterruptedCreation,
    /// It lists the feature, with a longer history.
    OrphanMirror,
    /// It decodes without the feature, with these bytes.
    Standard(Bytes),
    /// Not a regular file, or bytes that don't decode; why.
    Undecodable(String),
}

/// How a creation places `$ROOT/repo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Placement {
    /// An exclusive `link`: `$ROOT/repo` is missing.
    Link,
    /// A `rename` over an interrupted creation.
    Replace,
}

/// How a creation's directory rename ended.
#[derive(Debug, PartialEq, Eq)]
enum DirRename {
    Ours,
    /// Another creation won, with this current version if a listing showed
    /// one.
    Lost(Option<Version>),
}

impl RepoD<'_> {
    /// Creates the repository with `bytes`, which must list the feature, as
    /// its first version: its `N` is the Unix time in milliseconds. A
    /// repository, a standard repository or anything but an interrupted
    /// creation at `$ROOT/repo` is never overwritten.
    pub(crate) fn create(&self, bytes: &[u8]) -> Result<CreateOutcome, ProtocolError> {
        check_in_layout(bytes)?;
        fs::create_dir_all(self.root)?;
        nudge(self.root);
        let Some(placement) = self.placement()? else {
            return Ok(CreateOutcome::AlreadyExists);
        };

        let initial = Version::initial(unix_millis()?, Hash::of(bytes))?;
        let tmp = self.root.join(tmp_dir_name(Uuid::new_v4()));
        fs::DirBuilder::new().create(&tmp)?;
        let placed = write_fresh(&tmp.join(initial.file_name()), bytes, true)
            .map_err(ProtocolError::from)
            .and_then(|()| {
                self.hooks.at(Point::Creation(CreationStep::Built));
                self.place_mirror(bytes, placement)
            });
        match placed {
            Ok(true) => {}
            Ok(false) => {
                self.discard(&tmp);
                return Ok(CreateOutcome::AlreadyExists);
            }
            Err(err) => {
                self.discard(&tmp);
                return Err(err);
            }
        }
        self.hooks.at(Point::Creation(CreationStep::Placed));
        if let Err(err) = write_marker(&tmp, &initial) {
            self.discard(&tmp);
            return Err(err.into());
        }

        self.hooks.at(Point::Creation(CreationStep::BeforeDirRename));
        let renamed = self.hooks.rename(&tmp, &self.dir());
        match self.after_dir_rename(renamed, &tmp, &initial)? {
            DirRename::Ours => {}
            // `$ROOT/repo` may hold our version, which was never current.
            DirRename::Lost(current) => {
                match current {
                    Some(current) => {
                        let known = [current.hash().clone(), initial.hash().clone()];
                        self.log_unpublished(
                            &current,
                            self.publish(&current, None, &known),
                        );
                    }
                    None => tracing::warn!(
                        root = %self.root.display(),
                        "a creation lost its race and found no current version to \
                         publish: the mirror may hold a version that was never current"
                    ),
                }
                return Ok(CreateOutcome::AlreadyExists);
            }
        }
        self.hooks.at(Point::Creation(CreationStep::DirRenamed));
        let published = self.publish(&initial, Some(bytes), &[initial.hash().clone()]);
        self.log_unpublished(&initial, published);
        Ok(CreateOutcome::Created(initial))
    }

    /// The `open` classification. With `repo.d`, `$ROOT/repo` isn't read.
    pub(crate) fn open_status(&self) -> Result<OpenStatus, ProtocolError> {
        nudge(self.root);
        if self.repo_d_exists()? {
            return Ok(OpenStatus::Ok);
        }
        Ok(match self.root_repo() {
            Ok(RootRepo::Missing | RootRepo::InterruptedCreation) => OpenStatus::NotFound,
            Ok(RootRepo::OrphanMirror) => OpenStatus::OrphanMirror,
            Ok(RootRepo::Standard(_)) => OpenStatus::StandardLayout,
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
            OpenStatus::OrphanMirror => Err(ProtocolError::orphan_mirror(self.root)),
            OpenStatus::StandardLayout => Err(ProtocolError::standard_layout(self.root)),
            OpenStatus::Unreadable(why) => {
                Err(ProtocolError::repo_unreadable(self.root, &why))
            }
        }
    }

    /// Steps 2 and 3 of a creation: `None` if the repository exists or
    /// `$ROOT/repo` holds anything but an interrupted creation.
    fn placement(&self) -> Result<Option<Placement>, ProtocolError> {
        if self.repo_d_exists()? {
            return Ok(None);
        }
        Ok(match self.root_repo()? {
            RootRepo::Missing => Some(Placement::Link),
            RootRepo::InterruptedCreation => Some(Placement::Replace),
            RootRepo::Standard(_) => {
                return Err(ProtocolError::standard_layout(self.root));
            }
            RootRepo::OrphanMirror | RootRepo::Undecodable(_) => None,
        })
    }

    /// Writes `$ROOT/repo.staging.<id>` and places it as `$ROOT/repo`.
    /// `false` if another creator's `$ROOT/repo` is there: nothing was placed.
    fn place_mirror(
        &self,
        bytes: &[u8],
        placement: Placement,
    ) -> Result<bool, ProtocolError> {
        let repo = self.root.join(REPO);
        let staging = staging_path(&repo);
        write_fresh(&staging, bytes, true)?;
        let placed = self.place_staged(&staging, &repo, bytes, placement);
        // Gone after a rename.
        if let Err(err) = fs::remove_file(&staging)
            && !is_missing(&err)
        {
            tracing::debug!(%err, path = %staging.display(), "could not delete a staging file");
        }
        if let Ok(true) = placed {
            fsync_dir(self.root)?;
        }
        placed
    }

    fn place_staged(
        &self,
        staging: &Path,
        repo: &Path,
        bytes: &[u8],
        placement: Placement,
    ) -> Result<bool, ProtocolError> {
        if placement == Placement::Link {
            match self.hooks.link(staging, repo) {
                Ok(()) => return Ok(true),
                // Ours after a lost reply, or another creator's.
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                    if self.holds(bytes)? {
                        return Ok(true);
                    }
                    match self.placement()? {
                        Some(Placement::Replace) => {}
                        Some(Placement::Link) => {
                            return Err(io::Error::other(
                                "repo existed when linking it and was missing when read: \
                                 another process is moving it",
                            )
                            .into());
                        }
                        None => return Ok(false),
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
        match with_root_lock(self.root, || self.hooks.rename(staging, repo)) {
            Ok(()) => Ok(true),
            // Ours after a lost reply.
            Err(err) if err.kind() == io::ErrorKind::NotFound && self.holds(bytes)? => {
                Ok(true)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Whether `$ROOT/repo` holds exactly `bytes`.
    fn holds(&self, bytes: &[u8]) -> io::Result<bool> {
        let path = self.root.join(REPO);
        match with_root_lock(self.root, || read_all(&path)) {
            Ok(held) => Ok(held == bytes),
            Err(err) if is_missing(&err) => Ok(false),
            Err(_) if self.repo_is_not_a_file() => Ok(false),
            Err(err) => Err(err),
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
        let undecodable =
            |err| RootRepo::Undecodable(format!("it doesn't decode: {err}"));
        Ok(match layout_v1::lists_feature(&bytes) {
            Ok(false) => RootRepo::Standard(bytes),
            Ok(true) => match layout_v1::is_interrupted_creation(&bytes) {
                Ok(true) => RootRepo::InterruptedCreation,
                Ok(false) => RootRepo::OrphanMirror,
                Err(err) => undecodable(err),
            },
            Err(err) => undecodable(err),
        })
    }

    /// Step 7 of a creation, given the result of the directory rename.
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
                let current = self.listed_marker().unwrap_or_else(|err| {
                    tracing::debug!(
                        ?err,
                        "could not list repo.d after losing a creation"
                    );
                    None
                });
                Ok(DirRename::Lost(current.map(|marker| marker.version)))
            }
            // Our rename may have landed and its reply been lost: `tmp` is gone.
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                match self.listed_marker()? {
                    Some(marker) if marker.version == *initial => {
                        fsync_dir(self.root)?;
                        Ok(DirRename::Ours)
                    }
                    Some(marker) => Ok(DirRename::Lost(Some(marker.version))),
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

    /// A creation's publication logs what it couldn't do: the repository
    /// exists either way.
    fn log_unpublished(
        &self,
        version: &Version,
        published: Result<PublishOutcome, ProtocolError>,
    ) {
        let root = self.root.display();
        let version = version.file_name();
        match published {
            // `publish` warned of giving up.
            Ok(_) => {}
            Err(ProtocolError::Forked(_)) => tracing::error!(
                %root, %version,
                "a creation found {root}/{REPO} without {} and not a copy of a version \
                 of {root}/{REPO_D}, a fork: nothing was published",
                layout_v1::FEATURE
            ),
            Err(ProtocolError::MirrorUnreadable(_)) => tracing::error!(
                %root, %version,
                "a creation couldn't read or decode {root}/{REPO}: nothing was published"
            ),
            Err(err) => tracing::error!(
                %root, %version, ?err,
                "a creation couldn't publish the mirror {root}/{REPO}"
            ),
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
        os::unix::fs::MetadataExt as _,
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

    /// Exactly `$ROOT/repo`, a copy of `v0` holding `bytes`, and `repo.d`
    /// holding `v0` and its marker.
    fn assert_created(root: &Path, v0: &Version, bytes: &[u8]) {
        assert_eq!(names(root), strings(["repo", "repo.d"]));
        let dir = root.join("repo.d");
        assert_eq!(names(&dir), set([v0.file_name(), latest(v0)]));
        let version_file = dir.join(v0.file_name());
        assert_eq!(fs::read(&version_file).unwrap(), bytes);
        assert_eq!(fs::read(dir.join(latest(v0))).unwrap(), b"");
        assert_eq!(fs::read(root.join("repo")).unwrap(), bytes);
        assert_ne!(
            fs::metadata(root.join("repo")).unwrap().ino(),
            fs::metadata(version_file).unwrap().ino()
        );
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
    fn create_reaches_every_creation_point_then_publishes() {
        let root = tempfile::tempdir().unwrap();
        let hooks = TestHooks::default();
        create(root.path(), &hooks, &v0_bytes());
        // The publication finds the mirror in place and renames nothing.
        assert_eq!(
            hooks.reached(),
            [
                Point::Creation(CreationStep::Built),
                Point::Creation(CreationStep::Placed),
                Point::Creation(CreationStep::BeforeDirRename),
                Point::Creation(CreationStep::DirRenamed),
                Point::BeforePublish,
                Point::MirrorStaged,
                Point::BeforeMirrorRead,
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

    /// Only `RepoInitializedUpdate` in its ops log, like an interrupted
    /// creation, but without the feature.
    #[test]
    fn create_over_a_fresh_standard_repo_is_an_error() {
        let root = tempfile::tempdir().unwrap();
        plant(root.path(), &featureless_repo_info(3));
        let err = repo(root.path()).create(&v0_bytes()).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("is in the standard layout")),
            "{err:?}"
        );
        assert_eq!(names(root.path()), strings(["repo"]));
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), featureless_repo_info(3));
    }

    #[test]
    fn create_over_an_orphan_mirror_or_undecodable_bytes_exists() {
        for planted in [changed_repo_info(3), UNDECODABLE.to_vec(), Vec::new()] {
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
    fn create_over_an_unreadable_repo_file_is_an_error() {
        use std::os::unix::fs::PermissionsExt as _;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("repo");
        fs::write(&path, repo_info(3)).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
        // Root reads it anyway: nothing to test.
        if fs::read(&path).is_ok() {
            return;
        }
        let err = repo(root.path()).create(&v0_bytes()).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Io(err) if err.kind() == io::ErrorKind::PermissionDenied),
            "{err:?}"
        );
        assert_eq!(names(root.path()), strings(["repo"]));
    }

    #[test]
    fn create_while_repo_keeps_vanishing_is_an_error() {
        let root = tempfile::tempdir().unwrap();
        // The link sees a name, the read finds nothing behind it.
        std::os::unix::fs::symlink("missing", root.path().join("repo")).unwrap();
        let err = repo(root.path()).create(&v0_bytes()).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Io(err) if err.to_string().contains("another process is moving it")),
            "{err:?}"
        );
        assert_eq!(names(root.path()), strings(["repo"]));
    }

    #[test]
    fn create_resumes_an_interrupted_creation() {
        let root = tempfile::tempdir().unwrap();
        plant(root.path(), &repo_info(5));
        let v0 = create(root.path(), &NoHooks, &v0_bytes());
        assert_created(root.path(), &v0, &v0_bytes());
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

    /// Both found an interrupted creation; the loser replaced the winner's
    /// published mirror, lost the directory rename, and publishes the winner.
    #[test]
    fn a_loser_that_replaced_repo_publishes_the_winner() {
        let root = tempfile::tempdir().unwrap();
        plant(root.path(), &repo_info(9));
        let mut winner = None;
        let (lost, reached) = with_paused(
            TestHooks::default(),
            Point::Creation(CreationStep::Built),
            |hooks| RepoD { root: root.path(), hooks }.create(&repo_info(10)).unwrap(),
            || winner = Some(create(root.path(), &NoHooks, &repo_info(11))),
        );
        assert_eq!(lost, AlreadyExists);
        assert!(reached.contains(&Point::Creation(CreationStep::Placed)), "{reached:?}");
        assert!(reached.contains(&Point::BeforePublish), "{reached:?}");
        assert_created(root.path(), &winner.unwrap(), &repo_info(11));
    }

    /// The loser's link finds the winner's mirror: it places nothing.
    #[test]
    fn a_loser_that_finds_the_winners_repo_places_nothing() {
        let root = tempfile::tempdir().unwrap();
        let mut winner = None;
        let (lost, reached) = with_paused(
            TestHooks::default(),
            Point::Creation(CreationStep::Built),
            |hooks| RepoD { root: root.path(), hooks }.create(&repo_info(10)).unwrap(),
            || winner = Some(create(root.path(), &NoHooks, &repo_info(11))),
        );
        assert_eq!(lost, AlreadyExists);
        assert_eq!(reached, [Point::Creation(CreationStep::Built)]);
        assert_created(root.path(), &winner.unwrap(), &repo_info(11));
    }

    #[test]
    fn lost_replies_of_creation_steps_are_ours() {
        // The link of `$ROOT/repo`.
        let root = tempfile::tempdir().unwrap();
        let hooks = TestHooks::default().link(|from, to| {
            fs::hard_link(from, to)?;
            Err(io::ErrorKind::AlreadyExists.into())
        });
        let v0 = create(root.path(), &hooks, &v0_bytes());
        assert_created(root.path(), &v0, &v0_bytes());

        // The rename replacing an interrupted creation.
        let root = tempfile::tempdir().unwrap();
        plant(root.path(), &repo_info(9));
        let v0 = create(root.path(), &lose_rename_reply_onto("repo"), &v0_bytes());
        assert_created(root.path(), &v0, &v0_bytes());

        // The directory rename.
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
        // The interrupted creation the next creation resumes.
        assert_eq!(names(root.path()), strings(["repo"]));
        assert_eq!(repo(root.path()).open_status().unwrap(), OpenStatus::NotFound);
    }

    /// The directory rename reports `ENOENT` and the listing shows another
    /// creator's marker: it exists, after publishing the winner.
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
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), repo_info(11));
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
            Some(changed_repo_info(1)),
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
        // An interrupted creation.
        assert_eq!(status(&|root| plant(root, &repo_info(1))), OpenStatus::NotFound);
        assert_eq!(
            status(&|root| plant(root, &changed_repo_info(1))),
            OpenStatus::OrphanMirror
        );
        assert_eq!(
            status(&|root| plant(root, &featureless_repo_info(1))),
            OpenStatus::StandardLayout
        );
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
        let err = opens(&changed_repo_info(1)).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("an orphan mirror")),
            "{err:?}"
        );
        let err = opens(&featureless_repo_info(1)).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("is in the standard layout")),
            "{err:?}"
        );
        let err = opens(UNDECODABLE).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("can't be read or decoded and")),
            "{err:?}"
        );
        assert!(!opens(&repo_info(1)).unwrap());
    }
}
