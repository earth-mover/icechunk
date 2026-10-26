//! A change: stage the bytes, link them as the next version, rename the
//! marker from the base to it, then publish the mirror. See "Change" in
//! design-docs/020-filesystem-storage.md.

use std::{fs, io, path::Path};

use bytes::Bytes;
use icechunk_format::{
    OVERWRITTEN_FILES_PATH,
    layout_v1::{self, Attempt},
};
use uuid::Uuid;

use super::{
    Listed, OPS_LOG_FILES, ProtocolError, RepoD, is_dead, list_names,
    mirror::{Publication, PublishOutcome},
    read::Current,
    remove_stage, with_retries,
};
use crate::{
    hooks::{Observed, Point, Sabotage},
    names::{Hash, Marker, REPO, RepoDName, Version, stage_name},
    nudge::nudge,
    path::key_path,
    storage::is_missing,
    write::{fsync_dir, write_fresh},
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UpdateOutcome {
    Updated(Version),
    NotOnLatestVersion,
}

/// What an attempt of a change did, before its publication.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Attempted {
    pub(crate) outcome: UpdateOutcome,
    /// The current version the lost-reply check read, if it ran.
    pub(crate) read: Option<Version>,
}

impl From<UpdateOutcome> for Attempted {
    fn from(outcome: UpdateOutcome) -> Self {
        Self { outcome, read: None }
    }
}

/// What the lost-reply check proved about the attempt, with the current
/// version it read, if any.
enum Checked {
    /// An ops log names the attempt.
    Landed(Version),
    /// A version above the base has an ops log that provably omits it.
    Lost(Version),
    /// Neither: whether it landed is unknown.
    Unknown(Option<Version>),
}

impl Checked {
    fn attempted(self, new: Version) -> Attempted {
        let (outcome, read) = match self {
            Self::Landed(read) => (UpdateOutcome::Updated(new), Some(read)),
            Self::Lost(read) => (UpdateOutcome::NotOnLatestVersion, Some(read)),
            Self::Unknown(read) => (UpdateOutcome::NotOnLatestVersion, read),
        };
        Attempted { outcome, read }
    }
}

/// How a search of an ops log for an attempt ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Search {
    /// An entry names the attempt.
    Named,
    /// It didn't find the attempt, but passed the path right below it, or
    /// the end of the ops log: the attempt isn't in it.
    Absent,
    /// It stopped before either: a file is missing, or it read
    /// [`OPS_LOG_FILES`] files.
    Undecided,
}

/// Step 1 of a change: the new bytes list `filesystem-layout-v1`. Core adds
/// the storage's required features at creation and copies them on every
/// update, so bytes without it derive from a version that an interrupted
/// conversion to the standard layout made current.
pub(crate) fn check_change_bytes(bytes: &[u8]) -> Result<(), ProtocolError> {
    match layout_v1::lists_feature(bytes) {
        Ok(true) => Ok(()),
        Ok(false) => Err(ProtocolError::ConversionInProgress(format!(
            "the new repo info doesn't list {}: it derives from a version that an \
             interrupted conversion to the standard layout made current; nothing was \
             written",
            layout_v1::FEATURE
        ))),
        Err(err) => Err(ProtocolError::Other(format!(
            "the new repo info of a change doesn't decode, nothing was written: {err}"
        ))),
    }
}

impl RepoD<'_> {
    /// Changes version `base` to `new_bytes`, if `base` is current, then
    /// publishes the new version as the mirror before returning. A change
    /// that landed returns `Updated` whatever its publication did, logging
    /// what went wrong, except when the publication found a fork:
    /// [`ProtocolError::ForkedAfterUpdate`]. The caller checked the bytes with
    /// [`check_change_bytes`].
    pub(crate) fn change(
        &self,
        new_bytes: &[u8],
        base: &Version,
    ) -> Result<UpdateOutcome, ProtocolError> {
        let Attempted { outcome, read } = self.attempt_change(new_bytes, base)?;
        let UpdateOutcome::Updated(new) = outcome else {
            return Ok(outcome);
        };
        if self.hooks.sabotaged(Sabotage::CommitWithoutPublish) {
            return Ok(UpdateOutcome::Updated(new));
        }
        let known: Vec<Hash> =
            [base, &new].into_iter().chain(&read).map(|v| v.hash().clone()).collect();
        let mirror = self.root.join(REPO);
        let failed = match self.publish(&new, Some(new_bytes), &known) {
            Ok(Publication::Published(
                PublishOutcome::Current | PublishOutcome::GaveUp { .. },
            )) => None,
            Ok(Publication::Fork) => {
                return Err(ProtocolError::ForkedAfterUpdate {
                    detail: format!(
                        "{} doesn't list {} and isn't a copy of a version file of {}; \
                         nothing was published",
                        mirror.display(),
                        layout_v1::FEATURE,
                        self.dir().display()
                    ),
                    new,
                });
            }
            Ok(Publication::Unreadable) => Some(
                "it couldn't be read or decoded, so a fork can't be ruled out"
                    .to_string(),
            ),
            Err(err) => Some(format!("{err:?}")),
        };
        if let Some(why) = failed {
            tracing::error!(
                root = %self.root.display(),
                version = new.file_name(),
                "The change landed as version {} but publishing the mirror {} failed: \
                 {why}. The next change or GC publishes it; \
                 FilesystemStorage::publish_mirror() repairs it",
                new.file_name(),
                mirror.display(),
            );
        }
        Ok(UpdateOutcome::Updated(new))
    }

    /// Steps 2 to 4 of a change, without the publication. The fsyncs here
    /// always run: a marker naming truncated bytes is the one state nothing
    /// recovers from. The conversion to the standard layout calls it directly:
    /// it is the only caller that writes bytes without the feature.
    pub(super) fn attempt_change(
        &self,
        new_bytes: &[u8],
        base: &Version,
    ) -> Result<Attempted, ProtocolError> {
        let new = base.next(Hash::of(new_bytes))?;
        let dir = self.dir();
        let stage = dir.join(stage_name(Uuid::new_v4()));
        match write_fresh(&stage, new_bytes, true) {
            Err(err) if is_missing(&err) => {
                return Ok(UpdateOutcome::NotOnLatestVersion.into());
            }
            written => written?,
        }
        self.hooks.at(Point::Staged);

        let content = dir.join(new.file_name());
        let link_after_rename = self.hooks.sabotaged(Sabotage::LinkAfterRename);
        if !link_after_rename && !self.hooks.sabotaged(Sabotage::SkipLink) {
            match self.hooks.link(&stage, &content) {
                // The name is the hash of our bytes, so whatever holds it has
                // our bytes: typically our own link, retried after a lost reply.
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                    self.hooks.observed(Observed::LinkExisted);
                }
                // The staging file was swept, or `repo.d` is gone.
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    return Ok(UpdateOutcome::NotOnLatestVersion.into());
                }
                Err(err) => {
                    remove_stage(&stage);
                    return Err(err.into());
                }
            }
            // The link is durable before the marker can name it.
            fsync_dir(&dir).inspect_err(|_| remove_stage(&stage))?;
        }
        self.hooks.at(Point::Linked);
        if !link_after_rename {
            remove_stage(&stage);
        }
        if self.hooks.sabotaged(Sabotage::UnlinkBaseBeforeRename) {
            self.unlink_version(base);
        }
        self.hooks.at(Point::BeforeRename);

        let source = self.rename_source(base);
        let target = Marker { version: new.clone() };
        match self
            .hooks
            .rename(&dir.join(source.file_name()), &dir.join(target.file_name()))
        {
            Ok(()) => {
                self.hooks.at(Point::Renamed);
                if link_after_rename {
                    self.link_late(&stage, &content)?;
                }
                // Changed: from here an error means the change may not be durable.
                fsync_dir(&dir)?;
                self.hooks.at(Point::BeforeTidyUp);
                self.tidy_up(&new, base);
                Ok(UpdateOutcome::Updated(new).into())
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if self.hooks.sabotaged(Sabotage::TrustRenameEnoent) {
                    return Ok(UpdateOutcome::Updated(new).into());
                }
                self.after_failed_rename(new_bytes, base, new)
            }
            // The content file stays; the sweep owns it.
            Err(err) => Err(err.into()),
        }
    }

    /// [`RepoD::attempt_change`]'s outcome, for tests of the protocol that
    /// don't involve the mirror.
    #[cfg(test)]
    pub(crate) fn conditional_update(
        &self,
        new_bytes: &[u8],
        base: &Version,
    ) -> Result<UpdateOutcome, ProtocolError> {
        Ok(self.attempt_change(new_bytes, base)?.outcome)
    }

    /// The marker was not at our base, or our rename landed and its reply
    /// was lost: either way it left our base, so a listing that shows it at
    /// or below our base is stale. A listing that has caught up shows our
    /// marker, another `N+1` or a later one; past `N+1`, or without a fresh
    /// listing, the lost-reply check decides.
    fn after_failed_rename(
        &self,
        new_bytes: &[u8],
        base: &Version,
        new: Version,
    ) -> Result<Attempted, ProtocolError> {
        let dir = self.dir();
        let ack_unproven = || self.hooks.sabotaged(Sabotage::AckUnproven);
        match self.marker_past(base)? {
            Some(marker) if marker.version == new => {
                self.hooks.observed(Observed::RenameLanded);
                fsync_dir(&dir)?;
                self.tidy_up(&new, base);
                Ok(UpdateOutcome::Updated(new).into())
            }
            // Lost the race for `N+1`.
            Some(marker) if marker.version.n() == new.n() => {
                self.hooks.observed(Observed::RenameLostRace);
                let dead = if self.hooks.sabotaged(Sabotage::DeleteWinnerFile) {
                    &marker.version
                } else {
                    &new
                };
                self.unlink_version(dead);
                self.discard_backup(new_bytes);
                Ok(UpdateOutcome::NotOnLatestVersion.into())
            }
            // Superseded, whether or not our rename landed: our version file
            // is dead either way.
            Some(_) => {
                self.hooks.observed(Observed::RenameSuperseded);
                if ack_unproven()
                    || self.hooks.sabotaged(Sabotage::AckSupersededWithoutProof)
                {
                    return Ok(UpdateOutcome::Updated(new).into());
                }
                let checked = self.lost_reply_check(new_bytes, base, &new);
                self.unlink_version(&new);
                let checked = checked?;
                if let Checked::Lost(_) = checked {
                    self.discard_backup(new_bytes);
                }
                Ok(checked.attempted(new))
            }
            // Unproven: unless the check reads our version as current, our
            // version file is left to the sweep.
            None => {
                self.hooks.observed(Observed::RenameUnproven);
                if ack_unproven() {
                    return Ok(UpdateOutcome::Updated(new).into());
                }
                let checked = self.lost_reply_check(new_bytes, base, &new)?;
                if matches!(&checked, Checked::Landed(read) if *read == new) {
                    fsync_dir(&dir)?;
                    self.tidy_up(&new, base);
                }
                if let Checked::Lost(_) = checked {
                    self.discard_backup(new_bytes);
                }
                Ok(checked.attempted(new))
            }
        }
    }

    /// Deletes the backup of an attempt proven not to have landed: another
    /// change moved the marker past `N`, so `latest.<N>.<h>` is gone for good
    /// and our rename, even one retransmitted later, can't land; no version
    /// will name the backup. Errors are ignored.
    fn discard_backup(&self, new_bytes: &[u8]) {
        let backup = match layout_v1::attempt_id(new_bytes) {
            Ok(Some(backup)) => backup,
            Ok(None) => return,
            Err(err) => {
                tracing::debug!(%err, "could not read the attempt's backup path");
                return;
            }
        };
        let key = format!("{OVERWRITTEN_FILES_PATH}/{backup}");
        let path = match key_path(self.root, &key) {
            Ok(path) => path,
            Err(err) => {
                tracing::debug!(%err, backup, "the attempt names an invalid backup path");
                return;
            }
        };
        if let Err(err) = fs::remove_file(&path) {
            tracing::debug!(%err, path = %path.display(), "could not delete a backup");
        }
    }

    /// The highest marker of a listing of `repo.d` once it shows one above
    /// `base`, listing again while it doesn't, [`super::TRIES`] times at
    /// most. `None` if no listing did.
    fn marker_past(&self, base: &Version) -> Result<Option<Marker>, ProtocolError> {
        let dir = self.dir();
        with_retries(|| {
            nudge(&dir);
            match self.list_markers() {
                Ok(Listed::Markers(markers)) => Self::highest(&markers)
                    .filter(|marker| marker.version.n() > base.n())
                    .map(|marker| Ok(Some(marker.clone()))),
                Ok(Listed::NotFound) => None,
                Err(err) => Some(Err(err)),
            }
        })
        .unwrap_or(Ok(None))
    }

    /// The lost-reply check: whether the attempt `new` that wrote `new_bytes`
    /// became a version, although the marker isn't at it. Core draws a new
    /// backup path for every attempt and records it on the entry that was
    /// newest before the attempt, so it is in the ops log of the current
    /// version if and only if the attempt landed. Reads the current version,
    /// again while it is at or below `base` ([`super::TRIES`] times at most),
    /// then searches at most [`OPS_LOG_FILES`] files of its ops log. A read
    /// that fails is the error: whether the attempt landed is unknown. A
    /// conflict is proven only if the check read a version above `base`,
    /// which descends from the change at `N+1`, and its search decided.
    fn lost_reply_check(
        &self,
        new_bytes: &[u8],
        base: &Version,
        new: &Version,
    ) -> Result<Checked, ProtocolError> {
        let mut last = None;
        let fresh = with_retries(|| match self.read_latest() {
            Ok(Some(current)) if current.version.n() > base.n() => {
                Some(Ok(Some(current)))
            }
            Ok(current) => {
                last = current;
                None
            }
            Err(err) => Some(Err(err)),
        });
        let (current, fresh) = match fresh {
            Some(read) => (read?, true),
            None => (last, false),
        };
        let Some(Current { bytes, version }) = current else {
            return Ok(Checked::Unknown(None));
        };
        let search = if version == *new {
            Search::Named
        } else {
            match layout_v1::attempt(new_bytes).map_err(|err| self.undecodable(err))? {
                Some(Attempt { id, below }) => {
                    self.search_ops_log(bytes, &id, below.as_deref())?
                }
                None => Search::Undecided,
            }
        };
        Ok(match search {
            Search::Named => Checked::Landed(version),
            Search::Absent if fresh => Checked::Lost(version),
            Search::Absent | Search::Undecided => Checked::Unknown(Some(version)),
        })
    }

    /// Searches the backup paths of the ops log of `current`, newest first,
    /// for `attempt`, following `repo_before_updates` through at most
    /// [`OPS_LOG_FILES`] files. `below`, the path right below `attempt` in its
    /// own ops log, only decides [`Search::Absent`]: the search goes as far
    /// with it as without.
    fn search_ops_log(
        &self,
        current: Bytes,
        attempt: &str,
        below: Option<&str>,
    ) -> Result<Search, ProtocolError> {
        let mut file = current;
        let mut passed = false;
        for read in 1..=OPS_LOG_FILES {
            let named =
                layout_v1::backup_paths(&file).map_err(|err| self.undecodable(err))?;
            for path in named.entries.iter().chain(&named.repo_before_updates) {
                if path == attempt {
                    return Ok(Search::Named);
                }
                passed |= Some(path.as_str()) == below;
            }
            let Some(before) = named.repo_before_updates else {
                return Ok(Search::Absent);
            };
            if read == OPS_LOG_FILES {
                break;
            }
            match self.read_backup(&before, None)? {
                Some(bytes) => file = bytes,
                None => break,
            }
        }
        Ok(if passed { Search::Absent } else { Search::Undecided })
    }

    fn undecodable(&self, err: impl std::fmt::Display) -> ProtocolError {
        ProtocolError::Corrupt(format!(
            "a repo info of the ops log of {} doesn't decode: {err}",
            self.dir().display()
        ))
    }

    /// After a rename proved the marker is at `new`: deletes every dead
    /// version file, the same rule as the sweep's. Errors are ignored.
    fn tidy_up(&self, new: &Version, base: &Version) {
        self.unlink_version(base);
        if self.hooks.sabotaged(Sabotage::TidyUpDeletesCurrent) {
            self.unlink_version(new);
        }
        let delete_above = self.hooks.sabotaged(Sabotage::DeleteAboveMarker);
        let names = match list_names(&self.dir()) {
            Ok(names) => names,
            Err(err) => {
                tracing::debug!(%err, "could not list repo.d to tidy up");
                return;
            }
        };
        names
            .into_iter()
            .filter_map(|name| match name {
                RepoDName::Content(version) => Some(version),
                RepoDName::Marker(_) | RepoDName::Stage(_) | RepoDName::Mirror(_) => None,
            })
            .filter(|version| {
                is_dead(version, new) || (delete_above && version.n() > new.n())
            })
            .for_each(|version| self.unlink_version(&version));
    }

    /// The base's marker, or with [`Sabotage::RenameFromListedMarker`] the
    /// listing's highest `latest` marker.
    fn rename_source(&self, base: &Version) -> Marker {
        let base = Marker { version: base.clone() };
        if !self.hooks.sabotaged(Sabotage::RenameFromListedMarker) {
            return base;
        }
        nudge(&self.dir());
        match self.list_markers() {
            Ok(Listed::Markers(markers)) => markers
                .into_iter()
                .max_by_key(|marker| marker.version.n())
                .unwrap_or(base),
            Ok(Listed::NotFound) | Err(_) => base,
        }
    }

    /// [`Sabotage::LinkAfterRename`]: publishes the bytes the marker already
    /// names.
    fn link_late(&self, stage: &Path, content: &Path) -> Result<(), ProtocolError> {
        let linked = match self.hooks.link(stage, content) {
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => Ok(()),
            linked => linked,
        };
        remove_stage(stage);
        linked?;
        Ok(fsync_dir(&self.dir())?)
    }

    fn unlink_version(&self, version: &Version) {
        let path = self.dir().join(version.file_name());
        if let Err(err) = self.hooks.unlink(&path) {
            tracing::debug!(%err, path = %path.display(), "could not delete a version file");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        fs, io,
        path::{Path, PathBuf},
    };

    use tracing::Level;

    use super::{super::testing::*, UpdateOutcome::*, *};
    use crate::hooks::{NoHooks, Point, Sabotage};

    fn v1_bytes() -> Vec<u8> {
        repo_info(1)
    }

    fn other_bytes() -> Vec<u8> {
        repo_info(2)
    }

    const ALL_POINTS: [Point; 5] = [
        Point::Staged,
        Point::Linked,
        Point::BeforeRename,
        Point::Renamed,
        Point::BeforeTidyUp,
    ];

    fn update(
        root: &Path,
        hooks: &TestHooks,
        bytes: &[u8],
        base: &Version,
    ) -> Result<UpdateOutcome, ProtocolError> {
        RepoD { root, hooks }.conditional_update(bytes, base)
    }

    fn update_plain(root: &Path, bytes: &[u8], base: &Version) -> UpdateOutcome {
        RepoD { root, hooks: &NoHooks }.conditional_update(bytes, base).unwrap()
    }

    fn repo_d(root: &Path) -> PathBuf {
        root.join("repo.d")
    }

    /// `<N>.<h>` and the marker naming it.
    fn current(version: &Version) -> BTreeSet<String> {
        set([version.file_name(), latest(version)])
    }

    fn stages(dir: &Path) -> Vec<String> {
        names(dir).into_iter().filter(|name| name.starts_with("stage.")).collect()
    }

    /// Renames, then reports `ENOENT` as after a lost reply.
    fn lose_rename_reply(from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)?;
        Err(io::ErrorKind::NotFound.into())
    }

    /// Reports `ENOENT` without renaming: a stale client, or a lost reply of a
    /// rename another client's commit made fail.
    fn fail_rename(_: &Path, _: &Path) -> io::Result<()> {
        Err(io::ErrorKind::NotFound.into())
    }

    #[test]
    fn single_writer_commits() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default();
        let v1 = version(1, &v1_bytes());
        assert_eq!(
            update(root.path(), &hooks, &v1_bytes(), &v0).unwrap(),
            Updated(v1.clone())
        );
        assert_eq!(names(&repo_d(root.path())), current(&v1));
        assert_eq!(
            fs::read(repo_d(root.path()).join(v1.file_name())).unwrap(),
            v1_bytes()
        );
        assert_eq!(hooks.reached(), ALL_POINTS);

        let v2 = version(2, &v0_bytes());
        assert_eq!(update_plain(root.path(), &v0_bytes(), &v1), Updated(v2.clone()));
        assert_eq!(names(&repo_d(root.path())), current(&v2));
    }

    #[test]
    fn stale_base_conflicts_and_deletes_its_content() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v1 = version(1, &v1_bytes());
        assert_eq!(update_plain(root.path(), &v1_bytes(), &v0), Updated(v1.clone()));
        assert_eq!(update_plain(root.path(), &other_bytes(), &v0), NotOnLatestVersion);
        assert_eq!(names(&repo_d(root.path())), current(&v1));
    }

    #[test]
    fn loser_of_a_race_deletes_its_content() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let theirs = version(1, &other_bytes());
        let ours = version(1, &v1_bytes());
        let (result, _) = with_paused(
            TestHooks::default(),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, &v1_bytes(), &v0),
            || {
                // Another writer's commit, without its tidy-up.
                fs::write(dir.join(theirs.file_name()), other_bytes()).unwrap();
                fs::rename(dir.join(latest(&v0)), dir.join(latest(&theirs))).unwrap();
                assert!(dir.join(ours.file_name()).exists());
            },
        );
        assert_eq!(result.unwrap(), NotOnLatestVersion);
        assert_eq!(
            names(&dir),
            set([v0.file_name(), theirs.file_name(), latest(&theirs)])
        );
    }

    #[test]
    fn two_writers_on_one_base_one_wins() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let theirs = version(1, &other_bytes());
        let (result, reached) = with_paused(
            TestHooks::default(),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, &v1_bytes(), &v0),
            || {
                assert_eq!(
                    update_plain(root.path(), &other_bytes(), &v0),
                    Updated(theirs.clone())
                );
            },
        );
        assert_eq!(result.unwrap(), NotOnLatestVersion);
        assert_eq!(reached, [Point::Staged, Point::Linked, Point::BeforeRename]);
        assert_eq!(names(&repo_d(root.path())), current(&theirs));
    }

    #[test]
    fn lost_link_reply_is_our_own_link() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default().link(|from, to| {
            fs::hard_link(from, to)?;
            Err(io::ErrorKind::AlreadyExists.into())
        });
        let v1 = version(1, &v1_bytes());
        assert_eq!(
            update(root.path(), &hooks, &v1_bytes(), &v0).unwrap(),
            Updated(v1.clone())
        );
        assert_eq!(names(&repo_d(root.path())), current(&v1));
        assert_eq!(hooks.observed(), [Observed::LinkExisted]);
    }

    #[test]
    fn rename_after_another_commit_observes_the_lost_race() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let other_root = root.path().to_path_buf();
        let other_base = v0.clone();
        let hooks = TestHooks::default().rename(move |from, to| {
            update_plain(&other_root, &other_bytes(), &other_base);
            fs::rename(from, to)
        });
        assert_eq!(
            update(root.path(), &hooks, &v1_bytes(), &v0).unwrap(),
            NotOnLatestVersion
        );
        assert_eq!(hooks.observed(), [Observed::RenameLostRace]);
    }

    #[test]
    fn lost_rename_reply_is_a_success() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let stale = version(0, &other_bytes());
        fs::write(dir.join(stale.file_name()), other_bytes()).unwrap();
        let hooks = TestHooks::default().rename(lose_rename_reply);
        let v1 = version(1, &v1_bytes());
        assert_eq!(
            update(root.path(), &hooks, &v1_bytes(), &v0).unwrap(),
            Updated(v1.clone())
        );
        // Tidied up.
        assert_eq!(names(&dir), current(&v1));
        assert_eq!(hooks.observed(), [Observed::RenameLanded]);
    }

    #[test]
    fn marker_two_ahead_supersedes_the_attempt() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let v2 = version(2, &other_bytes());
        let (result, _) = with_paused(
            TestHooks::default(),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, &v1_bytes(), &v0),
            || {
                fs::write(dir.join(v2.file_name()), other_bytes()).unwrap();
                fs::rename(dir.join(latest(&v0)), dir.join(latest(&v2))).unwrap();
            },
        );
        assert_eq!(result.unwrap(), NotOnLatestVersion);
        assert_eq!(names(&dir), set([v0.file_name(), v2.file_name(), latest(&v2)]));
    }

    #[test]
    fn ambiguous_failure_conflicts_and_keeps_the_content() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default().rename(fail_rename);
        let v1 = version(1, &v1_bytes());
        assert_eq!(
            update(root.path(), &hooks, &v1_bytes(), &v0).unwrap(),
            NotOnLatestVersion
        );
        let dir = repo_d(root.path());
        assert_eq!(names(&dir), set([v0.file_name(), latest(&v0), v1.file_name()]));
        assert_eq!(hooks.observed(), [Observed::RenameUnproven]);
    }

    #[test]
    fn listing_error_after_a_failed_rename_is_an_error() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let replaced = dir.clone();
        let hooks = TestHooks::default().rename(move |_, _| {
            fs::remove_dir_all(&replaced)?;
            fs::write(&replaced, b"")?;
            Err(io::ErrorKind::NotFound.into())
        });
        let err = update(root.path(), &hooks, &v1_bytes(), &v0).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Io(err) if err.kind() == io::ErrorKind::NotADirectory),
            "{err:?}"
        );
        assert!(dir.is_file());
    }

    #[test]
    fn other_rename_errors_keep_the_content() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks =
            TestHooks::default().rename(|_, _| Err(io::Error::other("server down")));
        let err = update(root.path(), &hooks, &v1_bytes(), &v0).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Io(err) if err.to_string() == "server down")
        );
        let dir = repo_d(root.path());
        let v1 = version(1, &v1_bytes());
        assert_eq!(names(&dir), set([v0.file_name(), latest(&v0), v1.file_name()]));
    }

    #[test]
    fn other_link_errors_remove_the_stage() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default().link(|_, _| Err(io::Error::other("no links")));
        let err = update(root.path(), &hooks, &v1_bytes(), &v0).unwrap_err();
        assert!(matches!(&err, ProtocolError::Io(err) if err.to_string() == "no links"));
        assert_eq!(names(&repo_d(root.path())), current(&v0));
    }

    #[test]
    fn missing_repo_d_conflicts_and_creates_nothing() {
        let root = tempfile::tempdir().unwrap();
        let v0 = version(0, &v0_bytes());
        assert_eq!(update_plain(root.path(), &v1_bytes(), &v0), NotOnLatestVersion);
        assert!(names(root.path()).is_empty());
    }

    #[test]
    fn swept_stage_conflicts() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let sweep_dir = dir.clone();
        let hooks = TestHooks::default().at(move |point| {
            if point == Point::Staged {
                let stages = stages(&sweep_dir);
                assert_eq!(stages.len(), 1, "{stages:?}");
                fs::remove_file(sweep_dir.join(&stages[0])).unwrap();
            }
        });
        assert_eq!(
            update(root.path(), &hooks, &v1_bytes(), &v0).unwrap(),
            NotOnLatestVersion
        );
        assert_eq!(names(&dir), current(&v0));
    }

    #[test]
    fn same_bytes_as_the_base_are_a_new_version() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v1 = version(1, &v0_bytes());
        assert_eq!(update_plain(root.path(), &v0_bytes(), &v0), Updated(v1.clone()));
        assert_eq!(names(&repo_d(root.path())), current(&v1));
    }

    #[test]
    fn tidy_up_deletes_dead_versions_and_keeps_those_above() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let above = version(5, &other_bytes());
        let next_attempt = version(2, &other_bytes());
        let stage = format!("stage.{}", Uuid::new_v4());
        for name in [
            version(0, &other_bytes()).file_name(),
            version(1, &other_bytes()).file_name(),
            above.file_name(),
            next_attempt.file_name(),
            stage.clone(),
            ".nfs123".to_string(),
        ] {
            fs::write(dir.join(name), b"").unwrap();
        }
        let v1 = version(1, &v1_bytes());
        assert_eq!(update_plain(root.path(), &v1_bytes(), &v0), Updated(v1.clone()));
        let mut expected = current(&v1);
        expected.extend([above.file_name(), next_attempt.file_name(), stage]);
        expected.insert(".nfs123".to_string());
        assert_eq!(names(&dir), expected);
    }

    // ── The lost-reply check ────────────────────────────────────────────────

    use icechunk_format::repo_info::DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE as PER_FILE;

    fn attempt(
        root: &Path,
        hooks: &TestHooks,
        bytes: &[u8],
        base: &Version,
    ) -> Result<Attempted, ProtocolError> {
        RepoD { root, hooks }.attempt_change(bytes, base)
    }

    /// Commits from `base`, made by other writers without hooks: each one's
    /// bytes, its attempt `other.<i>`, its backup of the version it
    /// replaced in `overwritten/`, as core writes it. Returns the last version.
    fn commit_on_top(
        root: &Path,
        base: (&Version, &[u8]),
        commits: u32,
        per_file: u16,
    ) -> (Version, Vec<u8>) {
        let attempts: Vec<String> = (0..commits).map(|i| format!("other.{i}")).collect();
        commit_attempts(root, base, &attempts, 100, per_file)
    }

    /// [`commit_on_top`] with the attempts `attempts`, the first at `seq`.
    fn commit_attempts(
        root: &Path,
        base: (&Version, &[u8]),
        attempts: &[String],
        seq: u32,
        per_file: u16,
    ) -> (Version, Vec<u8>) {
        let (mut version, mut bytes) = (base.0.clone(), base.1.to_vec());
        fs::create_dir_all(root.join("overwritten")).unwrap();
        for (attempt, seq) in attempts.iter().zip(seq..) {
            fs::write(root.join("overwritten").join(attempt), &bytes).unwrap();
            bytes = committed_repo_info(&bytes, attempt, seq, per_file);
            version = match update_plain(root, &bytes, &version) {
                Updated(version) => version,
                NotOnLatestVersion => panic!("{version:?} is not current"),
            };
        }
        (version, bytes)
    }

    /// The backup of `base_bytes` that core writes for the attempt `attempt`
    /// before its change.
    fn back_up(root: &Path, attempt: &str, base_bytes: &[u8]) -> PathBuf {
        let dir = root.join("overwritten");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(attempt), base_bytes).unwrap();
        dir.join(attempt)
    }

    #[test]
    fn a_superseded_attempt_that_landed_is_acknowledged_through_the_ops_log() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let backup = back_up(root.path(), "ours", &v0_bytes());
        let ours = committed_repo_info(&v0_bytes(), "ours", 1, PER_FILE);
        let v1 = version(1, &ours);
        let (other_root, other_base, other_bytes) =
            (root.path().to_path_buf(), v1.clone(), ours.clone());
        let hooks = TestHooks::default().rename(move |from, to| {
            // Our rename lands; another writer commits before our listing.
            fs::rename(from, to)?;
            commit_on_top(&other_root, (&other_base, &other_bytes), 1, PER_FILE);
            Err(io::ErrorKind::NotFound.into())
        });
        let attempted = attempt(root.path(), &hooks, &ours, &v0).unwrap();
        let v2 = version(2, &committed_repo_info(&ours, "other.0", 100, PER_FILE));
        assert_eq!(attempted, Attempted { outcome: Updated(v1), read: Some(v2.clone()) });
        assert_eq!(hooks.observed(), [Observed::RenameSuperseded]);
        assert_eq!(names(&repo_d(root.path())), current(&v2));
        assert!(backup.exists());
    }

    /// Proven not to have landed: its backup is deleted.
    #[test]
    fn a_superseded_attempt_that_did_not_land_is_a_conflict() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let backup = back_up(root.path(), "ours", &v0_bytes());
        let ours = committed_repo_info(&v0_bytes(), "ours", 1, PER_FILE);
        let other_root = root.path().to_path_buf();
        let other_base = v0.clone();
        let hooks = TestHooks::default().rename(move |_, _| {
            // Two commits of another writer moved the marker from our base.
            commit_on_top(&other_root, (&other_base, &v0_bytes()), 2, PER_FILE);
            Err(io::ErrorKind::NotFound.into())
        });
        let attempted = attempt(root.path(), &hooks, &ours, &v0).unwrap();
        assert_eq!(attempted.outcome, NotOnLatestVersion);
        assert_eq!(attempted.read.map(|v| v.n()), Some(2));
        assert_eq!(hooks.observed(), [Observed::RenameSuperseded]);
        // Our version file is dead: deleted.
        assert_eq!(names(&repo_d(root.path())).len(), 2);
        assert!(!backup.exists());
        assert!(root.path().join("overwritten/other.1").exists());
    }

    /// The search passes the entry below the attempt's, which names the
    /// attempt that made the base, without finding the attempt: proven not
    /// landed, although the ops log goes on beyond the files it reads.
    #[test]
    fn a_superseded_attempt_the_search_passes_deletes_its_backup() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let earlier: Vec<String> = (0..4).map(|i| format!("earlier.{i}")).collect();
        let (base, base_bytes) =
            commit_attempts(root.path(), (&v0, &v0_bytes()), &earlier, 100, 2);
        let backup = back_up(root.path(), "ours", &base_bytes);
        let ours = committed_repo_info(&base_bytes, "ours", 200, 2);
        let (other_root, other_base, other_bytes) =
            (root.path().to_path_buf(), base.clone(), base_bytes.clone());
        let hooks = TestHooks::default().rename(move |_, _| {
            let later: Vec<String> = (0..2).map(|i| format!("later.{i}")).collect();
            commit_attempts(&other_root, (&other_base, &other_bytes), &later, 300, 2);
            Err(io::ErrorKind::NotFound.into())
        });
        let attempted = attempt(root.path(), &hooks, &ours, &base).unwrap();
        assert_eq!(attempted.outcome, NotOnLatestVersion);
        assert_eq!(hooks.observed(), [Observed::RenameSuperseded]);
        assert!(!backup.exists());
    }

    /// The search passes the entry below the attempt's, then the next
    /// ops-log file is missing: the entries newer than the one it passed are
    /// all it needed, so the attempt is proven not landed.
    #[test]
    fn a_search_that_passed_below_and_then_misses_a_file_deletes_the_backup() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let earlier: Vec<String> = (0..4).map(|i| format!("earlier.{i}")).collect();
        let (base, base_bytes) =
            commit_attempts(root.path(), (&v0, &v0_bytes()), &earlier, 100, 2);
        for name in &earlier {
            fs::remove_file(root.path().join("overwritten").join(name)).unwrap();
        }
        let backup = back_up(root.path(), "ours", &base_bytes);
        let ours = committed_repo_info(&base_bytes, "ours", 200, 2);
        let (other_root, other_base, other_bytes) =
            (root.path().to_path_buf(), base.clone(), base_bytes.clone());
        let hooks = TestHooks::default().rename(move |_, _| {
            let later: Vec<String> = (0..2).map(|i| format!("later.{i}")).collect();
            commit_attempts(&other_root, (&other_base, &other_bytes), &later, 300, 2);
            Err(io::ErrorKind::NotFound.into())
        });
        let attempted = attempt(root.path(), &hooks, &ours, &base).unwrap();
        assert_eq!(attempted.outcome, NotOnLatestVersion);
        assert_eq!(attempted.read.map(|v| v.n()), Some(base.n() + 2));
        assert_eq!(hooks.observed(), [Observed::RenameSuperseded]);
        assert!(!backup.exists());
    }

    #[test]
    fn a_lost_race_deletes_the_attempts_backup() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let backup = back_up(root.path(), "ours", &v0_bytes());
        let ours = committed_repo_info(&v0_bytes(), "ours", 1, PER_FILE);
        let (result, _) = with_paused(
            TestHooks::default(),
            Point::BeforeRename,
            |hooks| attempt(root.path(), hooks, &ours, &v0),
            || {
                commit_on_top(root.path(), (&v0, &v0_bytes()), 1, PER_FILE);
            },
        );
        assert_eq!(result.unwrap().outcome, NotOnLatestVersion);
        assert!(!backup.exists());
        assert!(root.path().join("overwritten/other.0").exists());
    }

    /// The deletion fails: the conflict is reported all the same.
    #[test]
    fn a_backup_that_cannot_be_deleted_is_still_a_conflict() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let backup = root.path().join("overwritten/ours");
        fs::create_dir_all(backup.join("not-a-file")).unwrap();
        let ours = committed_repo_info(&v0_bytes(), "ours", 1, PER_FILE);
        let (result, _) = with_paused(
            TestHooks::default(),
            Point::BeforeRename,
            |hooks| attempt(root.path(), hooks, &ours, &v0),
            || {
                commit_on_top(root.path(), (&v0, &v0_bytes()), 1, PER_FILE);
            },
        );
        assert_eq!(result.unwrap().outcome, NotOnLatestVersion);
        assert!(backup.is_dir());
    }

    /// An I/O error, a rename that failed while the listings never left the
    /// base, or a lost-reply check that couldn't read: whether the attempt
    /// landed is unknown, so its backup stays.
    #[test]
    fn an_attempt_whose_outcome_is_unknown_keeps_its_backup() {
        let failed_read = |root: &Path, v0: &Version| {
            let (other_root, other_base) = (root.to_path_buf(), v0.clone());
            TestHooks::default().rename(move |_, _| {
                let (theirs, _) =
                    commit_on_top(&other_root, (&other_base, &v0_bytes()), 2, PER_FILE);
                fs::write(other_root.join("repo.d").join(theirs.file_name()), b"torn")
                    .unwrap();
                Err(io::ErrorKind::NotFound.into())
            })
        };
        type MakeHooks = Box<dyn Fn(&Path, &Version) -> TestHooks>;
        let cases: [(&str, MakeHooks); 3] = [
            (
                "I/O error",
                Box::new(|_, _| {
                    TestHooks::default()
                        .rename(|_, _| Err(io::Error::other("server down")))
                }),
            ),
            ("unproven", Box::new(|_, _| TestHooks::default().rename(fail_rename))),
            ("failed read", Box::new(failed_read)),
        ];
        for (case, hooks) in cases {
            let root = tempfile::tempdir().unwrap();
            let v0 = seed(root.path());
            let backup = back_up(root.path(), "ours", &v0_bytes());
            let ours = committed_repo_info(&v0_bytes(), "ours", 1, PER_FILE);
            let attempted = attempt(root.path(), &hooks(root.path(), &v0), &ours, &v0);
            assert!(
                !matches!(&attempted, Ok(Attempted { outcome: Updated(_), .. })),
                "{case}: {attempted:?}"
            );
            assert!(backup.exists(), "{case}");
        }
    }

    /// With one entry per ops-log file, the attempt is named by the
    /// `repo_before_updates` of the file after its own: found within three
    /// files, not beyond. Its backup stays: beyond, the search can't tell.
    #[test]
    fn the_lost_reply_check_reads_at_most_three_ops_log_files() {
        for (commits, landed) in [(1, true), (2, true), (3, false)] {
            let root = tempfile::tempdir().unwrap();
            let v0 = seed(root.path());
            let backup = back_up(root.path(), "ours", &v0_bytes());
            let ours = committed_repo_info(&v0_bytes(), "ours", 1, 1);
            let v1 = version(1, &ours);
            let (other_root, other_base, other_bytes) =
                (root.path().to_path_buf(), v1.clone(), ours.clone());
            let hooks = TestHooks::default().rename(move |from, to| {
                fs::rename(from, to)?;
                commit_on_top(&other_root, (&other_base, &other_bytes), commits, 1);
                Err(io::ErrorKind::NotFound.into())
            });
            let attempted = attempt(root.path(), &hooks, &ours, &v0).unwrap();
            let expected = if landed { Updated(v1) } else { NotOnLatestVersion };
            assert_eq!(attempted.outcome, expected, "{commits} commits on top");
            assert!(backup.exists(), "{commits} commits on top");
        }
    }

    /// Hooks whose marker rename moves the marker out of `repo.d` and reports
    /// `ENOENT`: every relisting shows no marker, so the attempt is unproven.
    /// When the lost-reply check starts, the marker reappears as `restored`.
    fn hiding_the_marker(root: &Path, restored: String) -> TestHooks {
        let hidden = root.join("hidden-marker");
        let (hide, restore) = (hidden.clone(), root.join("repo.d").join(restored));
        TestHooks::default()
            .rename(move |from, _| {
                fs::rename(from, &hide)?;
                Err(io::ErrorKind::NotFound.into())
            })
            .when_observed(move |what| {
                if what == Observed::RenameUnproven {
                    fs::rename(&hidden, &restore).unwrap();
                }
            })
    }

    /// The listings never caught up, but the check reads our version as
    /// current: landed, so the directory is fsynced and tidied up.
    #[test]
    fn an_unproven_attempt_the_check_reads_as_current_is_landed() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let backup = back_up(root.path(), "ours", &v0_bytes());
        let ours = committed_repo_info(&v0_bytes(), "ours", 1, PER_FILE);
        let v1 = version(1, &ours);
        let hooks = hiding_the_marker(root.path(), latest(&v1));
        let attempted = attempt(root.path(), &hooks, &ours, &v0).unwrap();
        assert_eq!(
            attempted,
            Attempted { outcome: Updated(v1.clone()), read: Some(v1.clone()) }
        );
        assert_eq!(hooks.observed(), [Observed::RenameUnproven]);
        // The dead base is gone.
        assert_eq!(names(&repo_d(root.path())), current(&v1));
        assert!(backup.exists());
    }

    /// The listings never caught up, but the check reads another change's
    /// version above the base, whose ops log doesn't name the attempt: proven
    /// not landed, so its backup is deleted.
    #[test]
    fn an_unproven_attempt_the_check_proves_lost_deletes_its_backup() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let backup = back_up(root.path(), "ours", &v0_bytes());
        let ours = committed_repo_info(&v0_bytes(), "ours", 1, PER_FILE);
        let theirs =
            version(1, &committed_repo_info(&v0_bytes(), "other.0", 100, PER_FILE));
        let dir = repo_d(root.path());
        let hidden = root.path().join("hidden-marker");
        let (other_root, other_base, hide) =
            (root.path().to_path_buf(), v0.clone(), hidden.clone());
        let (marker, restore) = (dir.join(latest(&theirs)), dir.join(latest(&theirs)));
        let hooks = TestHooks::default()
            .rename(move |_, _| {
                commit_on_top(&other_root, (&other_base, &v0_bytes()), 1, PER_FILE);
                fs::rename(&marker, &hide)?;
                Err(io::ErrorKind::NotFound.into())
            })
            .when_observed(move |what| {
                if what == Observed::RenameUnproven {
                    fs::rename(&hidden, &restore).unwrap();
                }
            });
        let attempted = attempt(root.path(), &hooks, &ours, &v0).unwrap();
        assert_eq!(
            attempted,
            Attempted { outcome: NotOnLatestVersion, read: Some(theirs) }
        );
        assert_eq!(hooks.observed(), [Observed::RenameUnproven]);
        assert!(!backup.exists());
    }

    /// The marker never left our base: not landed, and our version file is
    /// left to the sweep. The check read no version above the base, so for
    /// all it knows the listings are stale: the backup stays.
    #[test]
    fn an_unproven_attempt_absent_from_the_ops_log_is_a_conflict() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let backup = back_up(root.path(), "ours", &v0_bytes());
        let ours = committed_repo_info(&v0_bytes(), "ours", 1, PER_FILE);
        let hooks = hiding_the_marker(root.path(), latest(&v0));
        let attempted = attempt(root.path(), &hooks, &ours, &v0).unwrap();
        assert_eq!(
            attempted,
            Attempted { outcome: NotOnLatestVersion, read: Some(v0.clone()) }
        );
        assert_eq!(hooks.observed(), [Observed::RenameUnproven]);
        let mut expected = current(&v0);
        expected.insert(version(1, &ours).file_name());
        assert_eq!(names(&repo_d(root.path())), expected);
        assert!(backup.exists());
    }

    #[test]
    fn a_failed_read_in_the_lost_reply_check_is_the_error() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let ours = committed_repo_info(&v0_bytes(), "ours", 1, PER_FILE);
        let other_root = root.path().to_path_buf();
        let other_base = v0.clone();
        let hooks = TestHooks::default().rename(move |_, _| {
            let (theirs, _) =
                commit_on_top(&other_root, (&other_base, &v0_bytes()), 2, PER_FILE);
            fs::write(other_root.join("repo.d").join(theirs.file_name()), b"torn")
                .unwrap();
            Err(io::ErrorKind::NotFound.into())
        });
        let err = attempt(root.path(), &hooks, &ours, &v0).unwrap_err();
        assert!(matches!(&err, ProtocolError::NoConsistentView(_)), "{err:?}");
    }

    // ── The publication after a change ──────────────────────────────────────

    fn change(
        root: &Path,
        hooks: &TestHooks,
        bytes: &[u8],
        base: &Version,
    ) -> Result<UpdateOutcome, ProtocolError> {
        RepoD { root, hooks }.change(bytes, base)
    }

    fn mirror(root: &Path) -> Option<Vec<u8>> {
        fs::read(root.join(REPO)).ok()
    }

    #[test]
    fn a_change_publishes_the_mirror_before_returning() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default();
        let v1 = version(1, &v1_bytes());
        assert_eq!(change(root.path(), &hooks, &v1_bytes(), &v0).unwrap(), Updated(v1));
        assert_eq!(mirror(root.path()), Some(v1_bytes()));
        let mut expected = ALL_POINTS.to_vec();
        expected.extend([
            Point::BeforePublish,
            Point::MirrorStaged,
            Point::BeforeMirrorRead,
            Point::BeforeMirrorRename,
            Point::MirrorRenamed,
        ]);
        assert_eq!(hooks.reached(), expected);
    }

    #[test]
    fn a_conflict_publishes_nothing() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        update_plain(root.path(), &v1_bytes(), &v0);
        let hooks = TestHooks::default();
        assert_eq!(
            change(root.path(), &hooks, &other_bytes(), &v0).unwrap(),
            NotOnLatestVersion
        );
        assert_eq!(mirror(root.path()), None);
        assert!(!hooks.reached().contains(&Point::BeforePublish));
    }

    #[test]
    fn a_fork_found_after_a_change_is_the_durable_error() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let fork = featureless_repo_info(9);
        fs::write(root.path().join(REPO), &fork).unwrap();
        let err =
            change(root.path(), &TestHooks::default(), &v1_bytes(), &v0).unwrap_err();
        let v1 = version(1, &v1_bytes());
        assert!(
            matches!(&err, ProtocolError::ForkedAfterUpdate { new, detail }
                if *new == v1 && detail.contains("nothing was published")),
            "{err:?}"
        );
        assert_eq!(names(&repo_d(root.path())), current(&v1));
        assert_eq!(mirror(root.path()), Some(fork));
    }

    /// An unreadable mirror and an I/O error of the publication: the change
    /// landed, so it is `Updated`, with the failure logged.
    #[test]
    fn a_landed_change_whose_publication_failed_is_updated_and_logged() {
        let unreadable = (TestHooks::default(), Some(Vec::new()));
        let failing_rename = (
            TestHooks::default().rename(|from, to| {
                if to.ends_with(REPO) {
                    Err(io::Error::other("mirror rename failed"))
                } else {
                    fs::rename(from, to)
                }
            }),
            None,
        );
        for (hooks, planted) in [unreadable, failing_rename] {
            let root = tempfile::tempdir().unwrap();
            let v0 = seed(root.path());
            if let Some(planted) = &planted {
                fs::write(root.path().join(REPO), planted).unwrap();
            }
            let (changed, errors) =
                logged(Level::ERROR, || change(root.path(), &hooks, &v1_bytes(), &v0));
            let v1 = version(1, &v1_bytes());
            assert_eq!(changed.unwrap(), Updated(v1.clone()));
            assert_eq!(mirror(root.path()), planted);
            let [error] = errors.as_slice() else { panic!("one error: {errors:?}") };
            assert!(error.contains(&v1.file_name()), "{error}");
            assert!(error.contains("FilesystemStorage::publish_mirror()"), "{error}");
            assert_eq!(names(&repo_d(root.path())), current(&v1));
        }
    }

    #[test]
    fn a_change_refuses_bytes_without_the_feature() {
        let err = check_change_bytes(&featureless_repo_info(1)).unwrap_err();
        assert!(matches!(&err, ProtocolError::ConversionInProgress(_)), "{err:?}");
        let err = check_change_bytes(b"not a repo info").unwrap_err();
        assert!(matches!(&err, ProtocolError::Other(_)), "{err:?}");
        check_change_bytes(&v1_bytes()).unwrap();
    }

    // ── Sabotages: each breaks the protocol in its own way ──────────────────

    #[test]
    fn sabotage_commit_without_publish_leaves_the_mirror_behind() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        fs::write(root.path().join(REPO), v0_bytes()).unwrap();
        let hooks = TestHooks::default().sabotage(Sabotage::CommitWithoutPublish);
        let v1 = version(1, &v1_bytes());
        assert_eq!(change(root.path(), &hooks, &v1_bytes(), &v0).unwrap(), Updated(v1));
        assert_eq!(mirror(root.path()), Some(v0_bytes()));
    }

    /// The acknowledged attempt is in no ops log: a change that didn't land.
    #[test]
    fn sabotage_ack_superseded_without_proof_acknowledges_a_change_that_did_not_land() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let ours = committed_repo_info(&v0_bytes(), "ours", 1, PER_FILE);
        let other_root = root.path().to_path_buf();
        let other_base = v0.clone();
        let hooks = TestHooks::default()
            .rename(move |_, _| {
                commit_on_top(&other_root, (&other_base, &v0_bytes()), 2, PER_FILE);
                Err(io::ErrorKind::NotFound.into())
            })
            .sabotage(Sabotage::AckSupersededWithoutProof);
        let attempted = attempt(root.path(), &hooks, &ours, &v0).unwrap();
        assert_eq!(attempted.outcome, Updated(version(1, &ours)));
        let current =
            RepoD { root: root.path(), hooks: &NoHooks }.read_latest().unwrap().unwrap();
        let named = layout_v1::backup_paths(&current.bytes).unwrap();
        assert!(!named.entries.contains(&"ours".to_string()), "{named:?}");
    }

    #[test]
    fn sabotage_skip_link_commits_a_marker_without_bytes() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default().sabotage(Sabotage::SkipLink);
        let v1 = version(1, &v1_bytes());
        assert_eq!(
            update(root.path(), &hooks, &v1_bytes(), &v0).unwrap(),
            Updated(v1.clone())
        );
        assert_eq!(names(&repo_d(root.path())), set([latest(&v1)]));
    }

    #[test]
    fn sabotage_rename_from_listed_marker_loses_an_update() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v1 = version(1, &v1_bytes());
        assert_eq!(update_plain(root.path(), &v1_bytes(), &v0), Updated(v1.clone()));
        let hooks = TestHooks::default().sabotage(Sabotage::RenameFromListedMarker);
        let overwrite = version(1, &other_bytes());
        assert_eq!(
            update(root.path(), &hooks, &other_bytes(), &v0).unwrap(),
            Updated(overwrite.clone())
        );
        // v1 is gone: replaced by an update based on v0.
        assert_eq!(names(&repo_d(root.path())), current(&overwrite));
    }

    #[test]
    fn sabotage_trust_rename_enoent_acknowledges_a_failed_commit() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default()
            .rename(fail_rename)
            .sabotage(Sabotage::TrustRenameEnoent);
        let v1 = version(1, &v1_bytes());
        assert_eq!(
            update(root.path(), &hooks, &v1_bytes(), &v0).unwrap(),
            Updated(v1.clone())
        );
        let dir = repo_d(root.path());
        assert_eq!(names(&dir), set([v0.file_name(), latest(&v0), v1.file_name()]));
    }

    #[test]
    fn sabotage_ack_unproven_acknowledges_ambiguous_failures() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks =
            TestHooks::default().rename(fail_rename).sabotage(Sabotage::AckUnproven);
        let v1 = version(1, &v1_bytes());
        assert_eq!(
            update(root.path(), &hooks, &v1_bytes(), &v0).unwrap(),
            Updated(v1.clone())
        );
        let dir = repo_d(root.path());
        assert_eq!(names(&dir), set([v0.file_name(), latest(&v0), v1.file_name()]));
    }

    #[test]
    fn sabotage_ack_unproven_acknowledges_superseded_attempts() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let v2 = version(2, &other_bytes());
        let (result, _) = with_paused(
            TestHooks::default().sabotage(Sabotage::AckUnproven),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, &v1_bytes(), &v0),
            || {
                fs::write(dir.join(v2.file_name()), other_bytes()).unwrap();
                fs::rename(dir.join(latest(&v0)), dir.join(latest(&v2))).unwrap();
            },
        );
        assert_eq!(result.unwrap(), Updated(version(1, &v1_bytes())));
        assert!(dir.join(latest(&v2)).exists());
    }

    #[test]
    fn sabotage_tidy_up_deletes_current_leaves_a_marker_without_bytes() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default().sabotage(Sabotage::TidyUpDeletesCurrent);
        let v1 = version(1, &v1_bytes());
        assert_eq!(
            update(root.path(), &hooks, &v1_bytes(), &v0).unwrap(),
            Updated(v1.clone())
        );
        assert_eq!(names(&repo_d(root.path())), set([latest(&v1)]));
    }

    #[test]
    fn sabotage_delete_above_marker_deletes_attempts_in_flight() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        fs::write(dir.join(version(2, &other_bytes()).file_name()), other_bytes())
            .unwrap();
        let hooks = TestHooks::default().sabotage(Sabotage::DeleteAboveMarker);
        let v1 = version(1, &v1_bytes());
        assert_eq!(
            update(root.path(), &hooks, &v1_bytes(), &v0).unwrap(),
            Updated(v1.clone())
        );
        assert_eq!(names(&dir), current(&v1));
    }

    #[test]
    fn sabotage_unlink_base_before_rename_leaves_the_current_marker_without_bytes() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let (result, _) = with_paused(
            TestHooks::default().sabotage(Sabotage::UnlinkBaseBeforeRename),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, &v1_bytes(), &v0),
            || {
                assert!(dir.join(latest(&v0)).exists());
                assert!(!dir.join(v0.file_name()).exists());
            },
        );
        assert_eq!(result.unwrap(), Updated(version(1, &v1_bytes())));
    }

    #[test]
    fn sabotage_link_after_rename_commits_a_marker_before_its_bytes() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let v1 = version(1, &v1_bytes());
        let (result, reached) = with_paused(
            TestHooks::default().sabotage(Sabotage::LinkAfterRename),
            Point::Renamed,
            |hooks| update(root.path(), hooks, &v1_bytes(), &v0),
            || {
                assert!(dir.join(latest(&v1)).exists());
                assert!(!dir.join(v1.file_name()).exists());
            },
        );
        assert_eq!(result.unwrap(), Updated(v1.clone()));
        assert_eq!(reached, ALL_POINTS);
        assert_eq!(names(&dir), current(&v1));
    }

    #[test]
    fn sabotage_delete_winner_file_leaves_the_winner_without_bytes() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let theirs = version(1, &other_bytes());
        let (result, _) = with_paused(
            TestHooks::default().sabotage(Sabotage::DeleteWinnerFile),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, &v1_bytes(), &v0),
            || {
                assert_eq!(
                    update_plain(root.path(), &other_bytes(), &v0),
                    Updated(theirs.clone())
                );
            },
        );
        assert_eq!(result.unwrap(), NotOnLatestVersion);
        assert_eq!(names(&repo_d(root.path())), set([latest(&theirs)]));
    }
}
