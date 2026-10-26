//! The mirror `$ROOT/repo`, a byte-identical copy of a version that was
//! current: the fork check and the publication, see "Publishing the mirror"
//! and "Forks" in design-docs/020-filesystem-storage.md.

use std::{
    collections::HashMap,
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex, PoisonError},
    thread,
    time::Duration,
};

use icechunk_format::layout_v1;
use icechunk_storage::VersionInfo;
use uuid::Uuid;

use super::{
    FIRST_BACKOFF, ProtocolError, RepoD, Step, TIME_SCALE, list_names,
    read::{Current, read_version_file},
};
use crate::{
    hooks::{Point, Sabotage},
    names::{Hash, REPO, RepoDName, Version, mirror_name},
    nudge::nudge,
    read::read_all,
    storage::is_missing,
    write::{fsync_dir, write_fresh},
};

/// The rounds of a publication before it gives up.
const ROUNDS: usize = 4;
/// The pause before the round after a busy rename: readers hold `$ROOT/repo`
/// open only briefly.
const BUSY_PAUSE: Duration = Duration::from_millis(20);
/// The re-reads of a `$ROOT/repo` that can't be read or decoded.
pub(super) const REREADS: u32 = 3;

/// What the fork check found in `$ROOT/repo`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ForkCheck {
    /// Missing, a copy of a version, or a lagging mirror. `copy_of` is the
    /// version file of the check's own listing whose bytes it holds.
    NotFork { copy_of: Option<Version> },
    /// A repo info without `filesystem-layout-v1` that is no version.
    Fork,
    /// It can't be read or decoded, so a fork can't be ruled out.
    Unreadable,
}

/// What a publication of the mirror `$ROOT/repo` that ran its course did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    /// `$ROOT/repo` holds the version current in the last listing of
    /// `repo.d`, or a newer one.
    Current,
    /// The rounds ran out before one installed the version its listing
    /// showed current: the publication stopped as a crash there would.
    /// `left` is the version it left `$ROOT/repo` holding, `None` if it
    /// installed none.
    GaveUp { left: Option<VersionInfo> },
}

/// What [`RepoD::publish`] did: each caller decides what a fork or an
/// unreadable mirror means for its operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Publication {
    Published(PublishOutcome),
    /// `$ROOT/repo` is a fork; nothing was published.
    Fork,
    /// `$ROOT/repo` couldn't be read or decoded; nothing was published.
    Unreadable,
}

/// What one round of a publication did to `$ROOT/repo`.
enum Installed {
    Renamed,
    /// `$ROOT/repo` already holds this version, the target or a newer one.
    InPlace(Version),
    /// The target's bytes or our stage were gone.
    Nothing,
    Refused(Publication),
    /// The rename failed with ETXTBSY: a process on this NFS client holds
    /// `$ROOT/repo` open. Nothing changed; the stage was removed.
    Busy(io::Error),
}

impl RepoD<'_> {
    /// Publishes `proved`, a version a server-decided step proved current,
    /// whose bytes the caller may pass. `known` holds hashes of repo infos
    /// the operation read: `$ROOT/repo` holding one of them is no fork.
    /// Forks and unreadable mirrors are outcomes; `Err` is an I/O failure.
    pub(crate) fn publish(
        &self,
        proved: &Version,
        proved_bytes: Option<&[u8]>,
        known: &[Hash],
    ) -> Result<Publication, ProtocolError> {
        self.hooks.at(Point::BeforePublish);
        let mut target = if self.hooks.sabotaged(Sabotage::PublishNonCurrent) {
            self.highest_version_file()?.unwrap_or_else(|| proved.clone())
        } else {
            proved.clone()
        };
        let mut left = None;
        for round in 1..=ROUNDS {
            let bytes = proved_bytes.filter(|_| target == *proved);
            let installed = match self.install(&target, bytes, known)? {
                Installed::Renamed => {
                    left = Some(target.clone());
                    true
                }
                Installed::InPlace(version) => {
                    target = version.clone();
                    left = Some(version);
                    true
                }
                Installed::Nothing => false,
                Installed::Refused(outcome) => return Ok(outcome),
                Installed::Busy(err) if round == ROUNDS => return Err(err.into()),
                Installed::Busy(_) => {
                    thread::sleep(BUSY_PAUSE / TIME_SCALE);
                    false
                }
            };
            let listed = self.listed_current()?;
            // Never backwards: a listing that lags ends the publication.
            if listed.n() > target.n() {
                target = listed;
            } else if installed {
                return Ok(Publication::Published(PublishOutcome::Current));
            }
        }
        let left_name = left.as_ref().map(Version::file_name);
        tracing::warn!(
            root = %self.root.display(),
            left = left_name.as_deref().unwrap_or("unknown"),
            "Gave up publishing the mirror {}/{REPO} after {ROUNDS} rounds, as a crash \
             there would: {}. FilesystemStorage::publish_mirror() repairs it",
            self.root.display(),
            match &left_name {
                Some(name) => format!("it holds version {name}, which may not be current"),
                None => "the version it holds is unknown".to_string(),
            }
        );
        Ok(Publication::Published(PublishOutcome::GaveUp {
            left: left.as_ref().map(Version::to_version_info),
        }))
    }

    /// One round: stage the target, check for a fork, rename the stage over
    /// `$ROOT/repo` and fsync the root.
    fn install(
        &self,
        target: &Version,
        bytes: Option<&[u8]>,
        known: &[Hash],
    ) -> Result<Installed, ProtocolError> {
        let dir = self.dir();
        let read;
        let bytes = match bytes {
            Some(bytes) => bytes,
            None => match read_version_file(&dir, target) {
                Ok(bytes) => {
                    read = bytes;
                    &read
                }
                // The marker moved on and the version was deleted.
                Err(err) if is_gone(&err) => return Ok(Installed::Nothing),
                Err(err) => return Err(err),
            },
        };
        // A new file, never a copy keeping the version file's mtime: the
        // sweep deletes mirror stages by age.
        let stage = dir.join(mirror_name(Uuid::new_v4()));
        write_fresh(&stage, bytes, true)?;
        self.hooks.at(Point::MirrorStaged);

        if !self.hooks.sabotaged(Sabotage::PublishWithoutForkCheck) {
            let known: Vec<Hash> = known.iter().chain([target.hash()]).cloned().collect();
            let (check, held) =
                self.check_mirror(&known).inspect_err(|_| self.remove(&stage))?;
            let done = match check {
                ForkCheck::Fork => Some(Installed::Refused(Publication::Fork)),
                ForkCheck::Unreadable => {
                    Some(Installed::Refused(Publication::Unreadable))
                }
                ForkCheck::NotFork { .. } if held.as_ref() == Some(target.hash()) => {
                    Some(Installed::InPlace(target.clone()))
                }
                ForkCheck::NotFork { copy_of: Some(version) }
                    if version.n() > target.n() =>
                {
                    Some(Installed::InPlace(version))
                }
                ForkCheck::NotFork { .. } => None,
            };
            if let Some(done) = done {
                self.remove(&stage);
                if matches!(done, Installed::InPlace(_)) {
                    // Another publisher's rename put it there, or ours whose
                    // reply was lost: neither may be durable yet, and GC
                    // deletes once this publication returns.
                    fsync_dir(self.root)?;
                }
                return Ok(done);
            }
        }
        self.hooks.at(Point::BeforeMirrorRename);

        let renamed = with_root_lock(self.root, || {
            self.hooks.rename(&stage, &self.root.join(REPO))
        });
        match renamed {
            Ok(()) => {}
            // Our stage is gone: swept, or our own rename whose reply was
            // lost. The verifying listing decides.
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(Installed::Nothing);
            }
            Err(err) if err.kind() == io::ErrorKind::ExecutableFileBusy => {
                self.remove(&stage);
                return Ok(Installed::Busy(err));
            }
            Err(err) => {
                self.remove(&stage);
                return Err(err.into());
            }
        }
        self.hooks.at(Point::MirrorRenamed);
        fsync_dir(self.root)?;
        Ok(Installed::Renamed)
    }

    /// The version of the marker of a listing of `repo.d`.
    fn listed_current(&self) -> Result<Version, ProtocolError> {
        self.with_listing(|marker| match marker {
            Some(marker) => Ok(Step::Done(marker.version.clone())),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} disappeared during a publication", self.dir().display()),
            )
            .into()),
        })
    }

    /// [`Sabotage::PublishNonCurrent`]: the version file with the highest `N`.
    fn highest_version_file(&self) -> Result<Option<Version>, ProtocolError> {
        Ok(list_names(&self.dir())?
            .into_iter()
            .filter_map(|name| match name {
                RepoDName::Content(version) => Some(version),
                RepoDName::Marker(_) | RepoDName::Stage(_) | RepoDName::Mirror(_) => None,
            })
            .max_by_key(Version::n))
    }

    /// Liveness only: the sweep deletes a mirror stage left behind.
    fn remove(&self, stage: &Path) {
        if let Err(err) = self.hooks.unlink(stage) {
            tracing::debug!(%err, path = %stage.display(), "could not delete a mirror stage");
        }
    }

    /// Classifies `$ROOT/repo`. `known` holds hashes of repo infos that the
    /// caller read or publishes. Reads that can't be decoded are retried
    /// [`REREADS`] times with backoff before `Unreadable`.
    pub(crate) fn fork_check(&self, known: &[Hash]) -> Result<ForkCheck, ProtocolError> {
        Ok(self.check_mirror(known)?.0)
    }

    /// [`RepoD::fork_check`], and the hash of the bytes `$ROOT/repo` held.
    fn check_mirror(
        &self,
        known: &[Hash],
    ) -> Result<(ForkCheck, Option<Hash>), ProtocolError> {
        let mut pause = FIRST_BACKOFF;
        for _ in 0..REREADS {
            if let Some(checked) = self.classify_mirror(known)? {
                return Ok(checked);
            }
            thread::sleep(pause / TIME_SCALE);
            pause *= 2;
        }
        Ok(self.classify_mirror(known)?.unwrap_or((ForkCheck::Unreadable, None)))
    }

    /// One read of `$ROOT/repo`, then one listing of `repo.d`. `None` if
    /// `$ROOT/repo` can't be read or decoded.
    fn classify_mirror(
        &self,
        known: &[Hash],
    ) -> Result<Option<(ForkCheck, Option<Hash>)>, ProtocolError> {
        // A cached negative entry would turn a fork into a missing file.
        nudge(self.root);
        let mirror = self.root.join(REPO);
        let read = || {
            self.hooks.at(Point::BeforeMirrorRead);
            read_all(&mirror)
        };
        let bytes = match with_root_lock(self.root, read) {
            Ok(bytes) => bytes,
            Err(err) if is_missing(&err) => {
                return Ok(Some((ForkCheck::NotFork { copy_of: None }, None)));
            }
            Err(err) if is_stale(&err) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let dir = self.dir();
        nudge(&dir);
        let hash = Hash::of(&bytes);
        let copy_of = list_names(&dir)?
            .into_iter()
            .filter_map(|name| match name {
                RepoDName::Content(version) if *version.hash() == hash => Some(version),
                _ => None,
            })
            .max_by_key(Version::n);
        let check = if copy_of.is_some() || known.contains(&hash) {
            ForkCheck::NotFork { copy_of }
        } else {
            match layout_v1::lists_feature(&bytes) {
                // A lagging mirror whose version file is gone.
                Ok(true) => ForkCheck::NotFork { copy_of: None },
                Ok(false) => ForkCheck::Fork,
                Err(err) => {
                    tracing::debug!(%err, path = %mirror.display(), "could not decode the mirror");
                    return Ok(None);
                }
            }
        };
        Ok(Some((check, Some(hash))))
    }

    /// `FilesystemStorage::publish_mirror`: publishes the listed current
    /// version. `None` if `repo.d` doesn't exist.
    pub(crate) fn publish_mirror(&self) -> Result<Option<PublishOutcome>, ProtocolError> {
        let Some(Current { bytes, version }) = self.read_latest()? else {
            return Ok(None);
        };
        let describe = || {
            format!(
                "the current version {} of {}",
                version.file_name(),
                self.dir().display()
            )
        };
        match layout_v1::lists_feature(&bytes) {
            Ok(true) => {}
            Ok(false) => {
                return Err(ProtocolError::ConversionInProgress(format!(
                    "{} doesn't list {}",
                    describe(),
                    layout_v1::FEATURE
                )));
            }
            Err(err) => {
                return Err(ProtocolError::Corrupt(format!(
                    "{} doesn't decode: {err}",
                    describe()
                )));
            }
        }
        let mirror = self.root.join(REPO);
        match self.publish(&version, Some(&bytes), &[version.hash().clone()])? {
            Publication::Fork => Err(ProtocolError::Forked(format!(
                "{} doesn't list {} and isn't a copy of a version file of {}; nothing was \
                 published",
                mirror.display(),
                layout_v1::FEATURE,
                self.dir().display()
            ))),
            Publication::Unreadable => Err(ProtocolError::MirrorUnreadable(format!(
                "{} couldn't be read or decoded; nothing was published",
                mirror.display()
            ))),
            Publication::Published(outcome) => Ok(Some(outcome)),
        }
    }
}

impl RepoD<'_> {
    /// GC's hook before its deletes: publishes `changed`, the version GC's
    /// change made. GC goes on once this publication left `$ROOT/repo`
    /// holding `changed` or a later version; otherwise nothing is deleted.
    pub(crate) fn publish_before_gc_deletes(
        &self,
        changed: &Version,
    ) -> Result<(), ProtocolError> {
        if self.hooks.sabotaged(Sabotage::GcWithoutForkCheck) {
            return Ok(());
        }
        let target = if self.hooks.sabotaged(Sabotage::GcPublishesListed) {
            self.listed_current()?
        } else {
            changed.clone()
        };
        let mirror = self.root.join(REPO);
        let mirror = mirror.display();
        match self.publish(&target, None, &[])? {
            Publication::Published(PublishOutcome::Current) => Ok(()),
            Publication::Published(PublishOutcome::GaveUp { left: Some(left) })
                if Version::from_version_info(&left)
                    .map_err(ProtocolError::Other)?
                    .n()
                    >= changed.n() =>
            {
                Ok(())
            }
            Publication::Published(PublishOutcome::GaveUp { .. }) => {
                Err(ProtocolError::Other(format!(
                    "the publication of the mirror {mirror} before GC's deletes gave up \
                     without installing version {} or a later one; nothing was deleted, \
                     run GC again",
                    changed.file_name()
                )))
            }
            Publication::Fork => Err(ProtocolError::Forked(format!(
                "{mirror} doesn't list {} and isn't a copy of a version file of {}; GC \
                 published nothing and deleted nothing",
                layout_v1::FEATURE,
                self.dir().display()
            ))),
            Publication::Unreadable => Err(ProtocolError::MirrorUnreadable(format!(
                "{mirror} couldn't be read or decoded; GC published nothing and deleted \
                 nothing"
            ))),
        }
    }

    /// GC's hook after its deletes: one more publication, from the listed
    /// version or from `changed` if the listing shows an older one. It
    /// repairs an older version a slow publisher renamed over the mirror
    /// during the deletes. GC has deleted already, so failures are logged.
    pub(crate) fn publish_after_gc_deletes(&self, changed: &Version) {
        let published = self.listed_current().and_then(|listed| {
            let target = if listed.n() > changed.n() { listed } else { changed.clone() };
            self.publish(&target, None, &[])
        });
        let failed = match published {
            Ok(Publication::Published(_)) => return,
            Ok(Publication::Fork) => "it is a fork".to_string(),
            Ok(Publication::Unreadable) => "it couldn't be read or decoded".to_string(),
            Err(err) => format!("{err:?}"),
        };
        tracing::error!(
            root = %self.root.display(),
            "Publishing the mirror {}/{REPO} after GC's deletes failed: {failed}. \
             FilesystemStorage::publish_mirror() repairs a mirror left behind",
            self.root.display(),
        );
    }

    /// GC's hook called without the version of GC's change: refused while
    /// `repo.d` exists. GC on a repository in the filesystem layout always
    /// makes a change first.
    pub(crate) fn refuse_gc_without_change(&self) -> Result<(), ProtocolError> {
        let Some(Current { bytes, version }) = self.read_latest()? else {
            return Ok(());
        };
        let current = format!(
            "the current version {} of {}",
            version.file_name(),
            self.dir().display()
        );
        match layout_v1::lists_feature(&bytes) {
            Ok(false) => Err(ProtocolError::ConversionInProgress(format!(
                "{current} doesn't list {}; GC deleted nothing",
                layout_v1::FEATURE
            ))),
            Ok(true) => Err(ProtocolError::Other(format!(
                "GC called the storage's hook without the version of its change, but \
                 {current} lists {}: a repository in the filesystem layout is collected \
                 only after GC's change; GC deleted nothing",
                layout_v1::FEATURE
            ))),
            Err(err) => {
                Err(ProtocolError::Corrupt(format!("{current} doesn't decode: {err}")))
            }
        }
    }
}

/// A version file that a change or a sweep deleted, or replaced under an NFS
/// reader.
pub(super) fn is_gone(err: &ProtocolError) -> bool {
    match err {
        ProtocolError::Io(err) => is_missing(err) || is_stale(err),
        ProtocolError::Corrupt(_) => true,
        _ => false,
    }
}

pub(super) fn is_stale(err: &io::Error) -> bool {
    err.raw_os_error() == Some(libc::ESTALE)
}

/// Runs `f` holding the lock this process takes around its reads of
/// `$ROOT/repo` and its renames onto it: a rename onto a file that is open on
/// the same NFS client becomes a silly rename.
pub(super) fn with_root_lock<T>(root: &Path, f: impl FnOnce() -> T) -> T {
    static LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
        LazyLock::new(Default::default);
    let key = fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let lock = Arc::clone(
        LOCKS.lock().unwrap_or_else(PoisonError::into_inner).entry(key).or_default(),
    );
    let _held = lock.lock().unwrap_or_else(PoisonError::into_inner);
    f()
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        os::unix::fs::MetadataExt as _,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::SystemTime,
    };

    use tracing::Level;

    use super::{super::testing::*, *};
    use crate::{
        hooks::NoHooks,
        repo_d::{commit::UpdateOutcome, sweep::sweep},
    };

    const GARBAGE: &[u8] = b"not a repo info";

    fn mirror(root: &Path) -> PathBuf {
        root.join(REPO)
    }

    fn mirror_stages(root: &Path) -> Vec<String> {
        names(&root.join("repo.d"))
            .into_iter()
            .filter(|name| name.starts_with("mirror."))
            .collect()
    }

    /// Changes the repo info from `base` to `bytes`, without hooks.
    fn change(root: &Path, bytes: &[u8], base: &Version) -> Version {
        let repo = RepoD { root, hooks: &NoHooks };
        match repo.conditional_update(bytes, base).unwrap() {
            UpdateOutcome::Updated(version) => version,
            UpdateOutcome::NotOnLatestVersion => panic!("{base:?} is not current"),
        }
    }

    fn publish(
        root: &Path,
        hooks: &TestHooks,
        proved: &Version,
        bytes: Option<&[u8]>,
    ) -> Publication {
        RepoD { root, hooks }.publish(proved, bytes, &[]).unwrap()
    }

    const CURRENT: Publication = Publication::Published(PublishOutcome::Current);

    fn mirror_steps() -> Vec<Point> {
        vec![
            Point::MirrorStaged,
            Point::BeforeMirrorRead,
            Point::BeforeMirrorRename,
            Point::MirrorRenamed,
        ]
    }

    #[test]
    fn publication_copies_the_proved_version() {
        for with_bytes in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let v0 = seed(root.path());
            let v1 = change(root.path(), &v1_bytes(), &v0);
            let hooks = TestHooks::default();
            let bytes = v1_bytes();
            let given = with_bytes.then_some(bytes.as_slice());
            assert_eq!(publish(root.path(), &hooks, &v1, given), CURRENT);
            assert_eq!(fs::read(mirror(root.path())).unwrap(), v1_bytes());
            assert!(mirror_stages(root.path()).is_empty());
            let mut expected = vec![Point::BeforePublish];
            expected.extend(mirror_steps());
            assert_eq!(hooks.reached(), expected);
            // A copy, never a link to the version file.
            let version_file = root.path().join("repo.d").join(v1.file_name());
            assert_ne!(
                fs::metadata(mirror(root.path())).unwrap().ino(),
                fs::metadata(version_file).unwrap().ino()
            );
        }
    }

    fn v1_bytes() -> Vec<u8> {
        repo_info(1)
    }

    #[test]
    fn fork_check_outcomes() {
        struct Case {
            name: &'static str,
            /// `$ROOT/repo`, if any.
            mirror: Option<Vec<u8>>,
            /// Version files to list besides `seed`'s.
            files: Vec<(u64, Vec<u8>)>,
            known: Vec<Vec<u8>>,
            expected: fn(&[Version]) -> ForkCheck,
        }
        let not_fork = |_: &[Version]| ForkCheck::NotFork { copy_of: None };
        let cases = [
            Case {
                name: "missing",
                mirror: None,
                files: vec![],
                known: vec![],
                expected: not_fork,
            },
            Case {
                // Shown without a decode: the bytes don't decode.
                name: "the copy of a listed version the caller never read",
                mirror: Some(GARBAGE.to_vec()),
                files: vec![(5, GARBAGE.to_vec())],
                known: vec![],
                expected: |files| ForkCheck::NotFork { copy_of: Some(files[0].clone()) },
            },
            Case {
                name: "the copy of a listed version without the feature",
                mirror: Some(featureless_repo_info(7)),
                files: vec![(7, featureless_repo_info(7))],
                known: vec![],
                expected: |files| ForkCheck::NotFork { copy_of: Some(files[0].clone()) },
            },
            Case {
                name: "a hash in known, without a listed version file",
                mirror: Some(GARBAGE.to_vec()),
                files: vec![],
                known: vec![GARBAGE.to_vec()],
                expected: not_fork,
            },
            Case {
                name: "a lagging mirror whose version file is gone",
                mirror: Some(repo_info(3)),
                files: vec![],
                known: vec![],
                expected: not_fork,
            },
            Case {
                name: "a fork",
                mirror: Some(featureless_repo_info(4)),
                files: vec![(4, repo_info(4))],
                known: vec![featureless_repo_info(5)],
                expected: |_| ForkCheck::Fork,
            },
            Case {
                name: "zero bytes",
                mirror: Some(Vec::new()),
                files: vec![],
                known: vec![],
                expected: |_| ForkCheck::Unreadable,
            },
        ];
        for case in cases {
            let root = tempfile::tempdir().unwrap();
            seed(root.path());
            let files: Vec<Version> = case
                .files
                .iter()
                .map(|(n, bytes)| {
                    let version = version(*n, bytes);
                    fs::write(
                        root.path().join("repo.d").join(version.file_name()),
                        bytes,
                    )
                    .unwrap();
                    version
                })
                .collect();
            if let Some(bytes) = &case.mirror {
                fs::write(mirror(root.path()), bytes).unwrap();
            }
            let known: Vec<Hash> =
                case.known.iter().map(|bytes| Hash::of(bytes)).collect();
            let check = RepoD { root: root.path(), hooks: &NoHooks }.fork_check(&known);
            assert_eq!(check.unwrap(), (case.expected)(&files), "{}", case.name);
            assert_eq!(fs::read(mirror(root.path())).ok(), case.mirror, "{}", case.name);
        }
    }

    #[test]
    fn a_fork_or_an_unreadable_mirror_is_never_overwritten() {
        for (planted, expected) in [
            (featureless_repo_info(9), Publication::Fork),
            (Vec::new(), Publication::Unreadable),
            (GARBAGE.to_vec(), Publication::Unreadable),
        ] {
            let root = tempfile::tempdir().unwrap();
            let v0 = seed(root.path());
            fs::write(mirror(root.path()), &planted).unwrap();
            let hooks = TestHooks::default();
            assert_eq!(publish(root.path(), &hooks, &v0, None), expected);
            assert_eq!(fs::read(mirror(root.path())).unwrap(), planted);
            assert!(mirror_stages(root.path()).is_empty());
            let reads = if expected == Publication::Fork { 1 } else { REREADS + 1 };
            let mut points = vec![Point::BeforePublish, Point::MirrorStaged];
            points.extend((0..reads).map(|_| Point::BeforeMirrorRead));
            assert_eq!(hooks.reached(), points);
        }
    }

    /// A fork written at `BeforeMirrorRead` is seen: every read of the fork
    /// check comes after the point.
    #[test]
    fn the_fork_check_reads_after_before_mirror_read() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let fork_root = root.path().to_path_buf();
        let hooks = TestHooks::default().at(move |point| {
            if point == Point::BeforeMirrorRead {
                fs::write(mirror(&fork_root), featureless_repo_info(9)).unwrap();
            }
        });
        assert_eq!(publish(root.path(), &hooks, &v0, None), Publication::Fork);
        assert_eq!(
            hooks.reached(),
            [Point::BeforePublish, Point::MirrorStaged, Point::BeforeMirrorRead]
        );

        // Once per read: an unreadable mirror is read `REREADS` more times.
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        fs::write(mirror(root.path()), GARBAGE).unwrap();
        let hooks = TestHooks::default();
        assert_eq!(publish(root.path(), &hooks, &v0, None), Publication::Unreadable);
        let mut expected = vec![Point::BeforePublish, Point::MirrorStaged];
        expected.extend([Point::BeforeMirrorRead; REREADS as usize + 1]);
        assert_eq!(hooks.reached(), expected);
    }

    #[test]
    fn a_newer_version_in_place_is_adopted() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        change(root.path(), &v1_bytes(), &v0);
        fs::write(mirror(root.path()), v1_bytes()).unwrap();
        let before = fs::metadata(mirror(root.path())).unwrap().ino();
        let hooks = TestHooks::default();
        // A publisher that proved v0 before v1 landed.
        assert_eq!(publish(root.path(), &hooks, &v0, Some(&v0_bytes())), CURRENT);
        assert_eq!(
            hooks.reached(),
            [Point::BeforePublish, Point::MirrorStaged, Point::BeforeMirrorRead]
        );
        assert_eq!(fs::metadata(mirror(root.path())).unwrap().ino(), before);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), v1_bytes());
        assert!(mirror_stages(root.path()).is_empty());
    }

    #[test]
    fn the_target_in_place_is_not_renamed_again_even_without_its_file() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        fs::write(mirror(root.path()), v0_bytes()).unwrap();
        // The tidy-up of this change deletes v0's file.
        let v1 = change(root.path(), &v1_bytes(), &v0);
        let hooks = TestHooks::default();
        assert_eq!(publish(root.path(), &hooks, &v0, Some(&v0_bytes())), CURRENT);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), v1_bytes());
        // Round 1 found v0 in place; round 2 renamed v1.
        let mut expected =
            vec![Point::BeforePublish, Point::MirrorStaged, Point::BeforeMirrorRead];
        expected.extend(mirror_steps());
        assert_eq!(hooks.reached(), expected);
        assert_eq!(
            names(&root.path().join("repo.d")),
            set([v1.file_name(), latest(&v1)])
        );
    }

    #[test]
    fn a_lagging_listing_never_sends_the_target_backwards() {
        let root = tempfile::tempdir().unwrap();
        seed(root.path());
        // Proved by a change whose marker rename the listing doesn't show yet.
        let v2 = version(2, &repo_info(2));
        fs::write(root.path().join("repo.d").join(v2.file_name()), repo_info(2)).unwrap();
        let hooks = TestHooks::default();
        assert_eq!(publish(root.path(), &hooks, &v2, None), CURRENT);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), repo_info(2));
    }

    /// Hooks that change the repo info every time the publication reaches
    /// `point`, at most `times` times.
    fn changing_at(root: &Path, base: Version, point: Point, times: usize) -> TestHooks {
        let root = root.to_path_buf();
        let base = Mutex::new(base);
        let changes = AtomicUsize::new(0);
        TestHooks::default().at(move |reached| {
            if reached == point && changes.fetch_add(1, Ordering::SeqCst) < times {
                let mut base = base.lock().unwrap();
                let seq = u32::try_from(base.n()).unwrap() + 1;
                *base = change(&root, &repo_info(seq), &base);
            }
        })
    }

    #[test]
    fn a_marker_moving_during_publication_publishes_the_newer_version() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = changing_at(root.path(), v0.clone(), Point::BeforeMirrorRename, 1);
        assert_eq!(publish(root.path(), &hooks, &v0, Some(&v0_bytes())), CURRENT);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), repo_info(1));
        let mut expected = vec![Point::BeforePublish];
        expected.extend(mirror_steps());
        expected.extend(mirror_steps());
        assert_eq!(hooks.reached(), expected);
        assert!(mirror_stages(root.path()).is_empty());
    }

    fn warnings<T>(f: impl FnOnce() -> T) -> (T, Vec<String>) {
        logged(Level::WARN, f)
    }

    #[test]
    fn four_rounds_then_it_gives_up_with_a_warning() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = changing_at(root.path(), v0.clone(), Point::BeforeMirrorRename, 4);
        let (outcome, logged) =
            warnings(|| publish(root.path(), &hooks, &v0, Some(&v0_bytes())));
        // Round 4 renamed v3; v4 landed meanwhile.
        let v3 = version(3, &repo_info(3));
        assert_eq!(
            outcome,
            Publication::Published(PublishOutcome::GaveUp {
                left: Some(v3.to_version_info())
            })
        );
        assert_eq!(fs::read(mirror(root.path())).unwrap(), repo_info(3));
        let renames = hooks
            .reached()
            .iter()
            .filter(|point| **point == Point::MirrorRenamed)
            .count();
        assert_eq!(renames, 4);
        let [warning] = logged.as_slice() else { panic!("one warning: {logged:?}") };
        assert!(warning.contains(&format!("left=\"{}\"", v3.file_name())), "{warning}");
        assert!(warning.contains("FilesystemStorage::publish_mirror()"), "{warning}");
    }

    #[test]
    fn giving_up_without_installing_says_the_mirror_is_unknown() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        // Every rename finds its stage gone, and the marker never moves.
        let hooks = TestHooks::default().rename(|from, _| {
            fs::remove_file(from)?;
            Err(io::ErrorKind::NotFound.into())
        });
        let (outcome, logged) = warnings(|| publish(root.path(), &hooks, &v0, None));
        assert_eq!(
            outcome,
            Publication::Published(PublishOutcome::GaveUp { left: None })
        );
        assert!(!mirror(root.path()).exists());
        let [warning] = logged.as_slice() else { panic!("one warning: {logged:?}") };
        assert!(warning.contains("left=\"unknown\""), "{warning}");
        assert!(warning.contains("the version it holds is unknown"), "{warning}");
    }

    #[test]
    fn enoent_on_the_rename_lets_the_listing_decide() {
        type Rename = Box<dyn Fn(&Path, &Path) -> io::Result<()> + Send + Sync>;
        // The stage was swept: the next round renames again. Our rename
        // landed and its reply was lost: the next round finds it in place.
        let swept: Rename = Box::new(|from, _| fs::remove_file(from));
        let lost_reply: Rename = Box::new(|from, to| fs::rename(from, to));
        for (first_rename, renamed_again) in [(swept, 1), (lost_reply, 0)] {
            let root = tempfile::tempdir().unwrap();
            let v0 = seed(root.path());
            let failed = AtomicBool::new(false);
            let hooks = TestHooks::default().rename(move |from, to| {
                if failed.swap(true, Ordering::SeqCst) {
                    fs::rename(from, to)
                } else {
                    first_rename(from, to)?;
                    Err(io::ErrorKind::NotFound.into())
                }
            });
            assert_eq!(publish(root.path(), &hooks, &v0, None), CURRENT);
            assert_eq!(fs::read(mirror(root.path())).unwrap(), v0_bytes());
            assert!(mirror_stages(root.path()).is_empty());
            let renamed =
                hooks.reached().iter().filter(|p| **p == Point::MirrorRenamed).count();
            assert_eq!(renamed, renamed_again);
        }
    }

    fn busy() -> io::Error {
        io::Error::from_raw_os_error(libc::ETXTBSY)
    }

    /// Hooks whose first `times` renames fail with ETXTBSY, counting every
    /// rename.
    fn busy_renames(times: usize) -> (TestHooks, Arc<AtomicUsize>) {
        let renames = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&renames);
        let hooks = TestHooks::default().rename(move |from, to| {
            if counted.fetch_add(1, Ordering::SeqCst) < times {
                Err(busy())
            } else {
                fs::rename(from, to)
            }
        });
        (hooks, renames)
    }

    #[test]
    fn a_busy_rename_is_retried_in_the_next_round() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let (hooks, renames) = busy_renames(1);
        assert_eq!(publish(root.path(), &hooks, &v0, None), CURRENT);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), v0_bytes());
        assert_eq!(renames.load(Ordering::SeqCst), 2);
        assert!(mirror_stages(root.path()).is_empty());
        // The second round staged and checked again.
        let mut expected = vec![
            Point::BeforePublish,
            Point::MirrorStaged,
            Point::BeforeMirrorRead,
            Point::BeforeMirrorRename,
        ];
        expected.extend(mirror_steps());
        assert_eq!(hooks.reached(), expected);
    }

    #[test]
    fn a_rename_busy_in_every_round_fails_with_etxtbsy() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let (hooks, renames) = busy_renames(usize::MAX);
        let (outcome, logged) = warnings(|| {
            RepoD { root: root.path(), hooks: &hooks }.publish(&v0, None, &[])
        });
        let err = outcome.unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Io(err) if err.kind() == io::ErrorKind::ExecutableFileBusy),
            "{err:?}"
        );
        assert_eq!(renames.load(Ordering::SeqCst), ROUNDS);
        assert!(logged.is_empty(), "{logged:?}");
        assert!(!mirror(root.path()).exists());
        assert!(mirror_stages(root.path()).is_empty());
    }

    #[test]
    fn any_other_rename_error_fails_the_first_round() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let renames = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&renames);
        let hooks = TestHooks::default().rename(move |_, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            Err(io::ErrorKind::PermissionDenied.into())
        });
        let err = RepoD { root: root.path(), hooks: &hooks }
            .publish(&v0, None, &[])
            .unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Io(err) if err.kind() == io::ErrorKind::PermissionDenied),
            "{err:?}"
        );
        assert_eq!(renames.load(Ordering::SeqCst), 1);
        assert!(mirror_stages(root.path()).is_empty());
    }

    #[test]
    fn a_marker_moving_during_a_busy_round_publishes_the_newer_version() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let (change_root, base) = (root.path().to_path_buf(), v0.clone());
        let changed = AtomicBool::new(false);
        let hooks = TestHooks::default().rename(move |from, to| {
            if changed.swap(true, Ordering::SeqCst) {
                fs::rename(from, to)
            } else {
                change(&change_root, &v1_bytes(), &base);
                Err(busy())
            }
        });
        assert_eq!(publish(root.path(), &hooks, &v0, Some(&v0_bytes())), CURRENT);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), v1_bytes());
        assert!(mirror_stages(root.path()).is_empty());
    }

    #[test]
    fn the_sweep_spares_a_mirror_stage_in_flight() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let version_file = root.path().join("repo.d").join(v0.file_name());
        File::open(&version_file)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(7200))
            .unwrap();
        let sweep_root = root.path().to_path_buf();
        let hooks = TestHooks::default().at(move |point| {
            if point == Point::MirrorStaged {
                let cutoff = SystemTime::now() - Duration::from_secs(3600);
                sweep(&sweep_root, cutoff, false, &NoHooks).unwrap();
                assert_eq!(mirror_stages(&sweep_root).len(), 1);
            }
        });
        assert_eq!(publish(root.path(), &hooks, &v0, None), CURRENT);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), v0_bytes());
    }

    /// The conversion to the standard layout's change and publication, landing
    /// while `publish_mirror()` runs from the listing before it.
    #[test]
    fn publish_mirror_never_renames_an_older_version_over_a_featureless_one() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        fs::write(mirror(root.path()), v0_bytes()).unwrap();
        let converter_root = root.path().to_path_buf();
        let converted = AtomicBool::new(false);
        let hooks = TestHooks::default().at(move |point| {
            if point == Point::MirrorStaged && !converted.swap(true, Ordering::SeqCst) {
                change(&converter_root, &featureless_repo_info(1), &v0);
                fs::write(mirror(&converter_root), featureless_repo_info(1)).unwrap();
            }
        });
        let outcome =
            RepoD { root: root.path(), hooks: &hooks }.publish_mirror().unwrap();
        assert_eq!(outcome, Some(PublishOutcome::Current));
        assert_eq!(fs::read(mirror(root.path())).unwrap(), featureless_repo_info(1));
        assert!(!hooks.reached().contains(&Point::BeforeMirrorRename));
        assert!(mirror_stages(root.path()).is_empty());
    }

    /// A stale listing in the fork check: hiding the version file has the same
    /// effect on it.
    #[test]
    fn a_fork_check_listing_that_misses_the_version_reports_a_fork() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let converted = change(root.path(), &featureless_repo_info(1), &v0);
        fs::write(mirror(root.path()), featureless_repo_info(1)).unwrap();
        let dir = root.path().join("repo.d");
        let (file, hidden) =
            (dir.join(converted.file_name()), root.path().join("hidden"));
        let (hide, unhide) = (file.clone(), hidden.clone());
        let hooks = TestHooks::default().at(move |point| {
            if point == Point::MirrorStaged {
                fs::rename(&hide, &unhide).unwrap();
            }
        });
        // A publisher whose listing showed v0, with its bytes.
        let repo = RepoD { root: root.path(), hooks: &hooks };
        let outcome = repo.publish(&v0, Some(&v0_bytes()), &[v0.hash().clone()]).unwrap();
        assert_eq!(outcome, Publication::Fork);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), featureless_repo_info(1));
        assert!(mirror_stages(root.path()).is_empty());

        fs::rename(&hidden, &file).unwrap();
        let fresh = RepoD { root: root.path(), hooks: &NoHooks };
        let outcome =
            fresh.publish(&v0, Some(&v0_bytes()), &[v0.hash().clone()]).unwrap();
        assert_eq!(outcome, CURRENT);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), featureless_repo_info(1));
    }

    #[test]
    fn publish_mirror_repairs_a_lagging_mirror() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        fs::write(mirror(root.path()), v0_bytes()).unwrap();
        change(root.path(), &v1_bytes(), &v0);
        let repo = RepoD { root: root.path(), hooks: &NoHooks };
        assert_eq!(repo.publish_mirror().unwrap(), Some(PublishOutcome::Current));
        assert_eq!(fs::read(mirror(root.path())).unwrap(), v1_bytes());
    }

    #[test]
    fn publish_mirror_refuses_forks_unreadable_mirrors_and_featureless_versions() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let repo = RepoD { root: root.path(), hooks: &NoHooks };
        let mirror_path = mirror(root.path()).display().to_string();
        let forked = |err: &ProtocolError| matches!(err, ProtocolError::Forked(detail) if detail.contains(&mirror_path));
        let unreadable =
            |err: &ProtocolError| matches!(err, ProtocolError::MirrorUnreadable(_));
        type Refused<'a> = &'a dyn Fn(&ProtocolError) -> bool;
        let cases: [(Vec<u8>, Refused<'_>); 2] =
            [(featureless_repo_info(9), &forked), (Vec::new(), &unreadable)];
        for (planted, refused) in cases {
            fs::write(mirror(root.path()), &planted).unwrap();
            let err = repo.publish_mirror().unwrap_err();
            assert!(refused(&err), "{err:?}");
            assert_eq!(fs::read(mirror(root.path())).unwrap(), planted);
            assert!(mirror_stages(root.path()).is_empty());
        }

        fs::remove_file(mirror(root.path())).unwrap();
        change(root.path(), &featureless_repo_info(1), &v0);
        let err = repo.publish_mirror().unwrap_err();
        assert!(matches!(&err, ProtocolError::ConversionInProgress(_)), "{err:?}");
        assert!(!mirror(root.path()).exists());
        assert!(mirror_stages(root.path()).is_empty());
    }

    #[test]
    fn publish_mirror_without_repo_d_is_none() {
        let root = tempfile::tempdir().unwrap();
        let repo = RepoD { root: root.path(), hooks: &NoHooks };
        assert_eq!(repo.publish_mirror().unwrap(), None);
        assert!(names(root.path()).is_empty());
    }

    // ── GC's hook ───────────────────────────────────────────────────────────

    fn repo<'a>(root: &'a Path, hooks: &'a TestHooks) -> RepoD<'a> {
        RepoD { root, hooks }
    }

    #[test]
    fn before_gc_deletes_the_version_of_gcs_change_is_published() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        fs::write(mirror(root.path()), v0_bytes()).unwrap();
        let v1 = change(root.path(), &v1_bytes(), &v0);
        repo(root.path(), &TestHooks::default()).publish_before_gc_deletes(&v1).unwrap();
        assert_eq!(fs::read(mirror(root.path())).unwrap(), v1_bytes());
    }

    #[test]
    fn before_gc_deletes_a_fork_or_an_unreadable_mirror_is_refused() {
        for planted in [featureless_repo_info(9), Vec::new()] {
            let root = tempfile::tempdir().unwrap();
            let v0 = seed(root.path());
            fs::write(mirror(root.path()), &planted).unwrap();
            let err = repo(root.path(), &TestHooks::default())
                .publish_before_gc_deletes(&v0)
                .unwrap_err();
            match &err {
                ProtocolError::Forked(detail) => {
                    assert!(!planted.is_empty());
                    assert!(detail.contains("deleted nothing"), "{detail}");
                }
                ProtocolError::MirrorUnreadable(detail) => {
                    assert!(planted.is_empty());
                    assert!(detail.contains("deleted nothing"), "{detail}");
                }
                err => panic!("{err:?}"),
            }
            assert_eq!(fs::read(mirror(root.path())).unwrap(), planted);
        }
    }

    /// Giving up at or above GC's version lets GC go on; giving up without
    /// installing anything fails it, naming the rerun.
    #[test]
    fn before_gc_deletes_a_publication_that_gave_up_goes_on_only_at_or_above_gcs_version()
    {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = changing_at(root.path(), v0.clone(), Point::BeforeMirrorRename, 4);
        let (gone_on, _) =
            warnings(|| repo(root.path(), &hooks).publish_before_gc_deletes(&v0));
        gone_on.unwrap();
        assert_eq!(fs::read(mirror(root.path())).unwrap(), repo_info(3));

        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default().rename(|from, _| {
            fs::remove_file(from)?;
            Err(io::ErrorKind::NotFound.into())
        });
        let (failed, _) =
            warnings(|| repo(root.path(), &hooks).publish_before_gc_deletes(&v0));
        let err = failed.unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("run GC again")),
            "{err:?}"
        );
        assert!(!mirror(root.path()).exists());
    }

    #[test]
    fn after_gc_deletes_a_regressed_mirror_is_repaired() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v1 = change(root.path(), &v1_bytes(), &v0);
        // A slow publisher renamed v0 over the mirror during the deletes.
        fs::write(mirror(root.path()), v0_bytes()).unwrap();
        repo(root.path(), &TestHooks::default()).publish_after_gc_deletes(&v1);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), v1_bytes());

        // A later change is published too.
        let v2 = change(root.path(), &repo_info(2), &v1);
        fs::write(mirror(root.path()), v0_bytes()).unwrap();
        repo(root.path(), &TestHooks::default()).publish_after_gc_deletes(&v1);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), repo_info(2));
        assert!(v2.n() > v1.n());
    }

    /// A listing that lags behind GC's change never sends the mirror below
    /// GC's version.
    #[test]
    fn after_gc_deletes_never_publishes_below_gcs_version() {
        let root = tempfile::tempdir().unwrap();
        seed(root.path());
        fs::write(mirror(root.path()), v0_bytes()).unwrap();
        let v2 = version(2, &repo_info(2));
        fs::write(root.path().join("repo.d").join(v2.file_name()), repo_info(2)).unwrap();
        repo(root.path(), &TestHooks::default()).publish_after_gc_deletes(&v2);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), repo_info(2));
    }

    #[test]
    fn after_gc_deletes_failures_are_logged() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let fork = featureless_repo_info(9);
        fs::write(mirror(root.path()), &fork).unwrap();
        let ((), errors) = logged(Level::ERROR, || {
            repo(root.path(), &TestHooks::default()).publish_after_gc_deletes(&v0);
        });
        let [error] = errors.as_slice() else { panic!("one error: {errors:?}") };
        assert!(error.contains("it is a fork"), "{error}");
        assert_eq!(fs::read(mirror(root.path())).unwrap(), fork);
    }

    #[test]
    fn gc_without_a_change_is_refused_while_repo_d_exists() {
        let root = tempfile::tempdir().unwrap();
        let hooks = TestHooks::default();
        repo(root.path(), &hooks).refuse_gc_without_change().unwrap();

        let v0 = seed(root.path());
        let err = repo(root.path(), &hooks).refuse_gc_without_change().unwrap_err();
        assert!(matches!(&err, ProtocolError::Other(_)), "{err:?}");

        change(root.path(), &featureless_repo_info(1), &v0);
        let err = repo(root.path(), &hooks).refuse_gc_without_change().unwrap_err();
        assert!(matches!(&err, ProtocolError::ConversionInProgress(_)), "{err:?}");
        assert!(!mirror(root.path()).exists());
    }

    // ── Sabotages ───────────────────────────────────────────────────────────

    #[test]
    fn sabotage_gc_without_fork_check_goes_on_over_a_fork() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let fork = featureless_repo_info(9);
        fs::write(mirror(root.path()), &fork).unwrap();
        let hooks = TestHooks::default().sabotage(Sabotage::GcWithoutForkCheck);
        repo(root.path(), &hooks).publish_before_gc_deletes(&v0).unwrap();
        assert_eq!(fs::read(mirror(root.path())).unwrap(), fork);
    }

    /// GC's change proved v2 current; a listing that lags shows v0.
    #[test]
    fn sabotage_gc_publishes_listed_publishes_an_older_version_than_gcs() {
        let root = tempfile::tempdir().unwrap();
        seed(root.path());
        let v2 = version(2, &repo_info(2));
        fs::write(root.path().join("repo.d").join(v2.file_name()), repo_info(2)).unwrap();
        let hooks = TestHooks::default().sabotage(Sabotage::GcPublishesListed);
        repo(root.path(), &hooks).publish_before_gc_deletes(&v2).unwrap();
        assert_eq!(fs::read(mirror(root.path())).unwrap(), v0_bytes());
    }

    #[test]
    fn sabotage_publish_without_fork_check_overwrites_a_fork() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        fs::write(mirror(root.path()), featureless_repo_info(9)).unwrap();
        let hooks = TestHooks::default().sabotage(Sabotage::PublishWithoutForkCheck);
        assert_eq!(publish(root.path(), &hooks, &v0, None), CURRENT);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), v0_bytes());
        assert!(hooks.reached().contains(&Point::BeforeMirrorRename));
        assert!(!hooks.reached().contains(&Point::BeforeMirrorRead));
    }

    #[test]
    fn sabotage_publish_non_current_publishes_a_version_above_the_marker() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let attempt = version(3, &repo_info(3));
        fs::write(root.path().join("repo.d").join(attempt.file_name()), repo_info(3))
            .unwrap();
        let hooks = TestHooks::default().sabotage(Sabotage::PublishNonCurrent);
        assert_eq!(publish(root.path(), &hooks, &v0, None), CURRENT);
        assert_eq!(fs::read(mirror(root.path())).unwrap(), repo_info(3));
        assert!(root.path().join("repo.d").join(latest(&v0)).exists());
    }
}
