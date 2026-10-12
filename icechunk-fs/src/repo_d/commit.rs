//! The commit: stage the bytes, link them as the next version, rename the
//! marker from the base to it.

use std::{io, path::Path};

use uuid::Uuid;

use super::{Listed, ProtocolError, RepoD, is_dead, list_names, remove_stage};
use crate::{
    hooks::{Observed, Point, Sabotage},
    names::{Hash, Marker, MarkerKind, RepoDName, Version, stage_name},
    nudge::nudge,
    storage::is_missing,
    write::{fsync_dir, write_fresh},
};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UpdateOutcome {
    Updated(Version),
    NotOnLatestVersion,
}

impl RepoD<'_> {
    /// Replaces version `base` with `new_bytes`, if `base` is current. The
    /// fsyncs here always run: a marker naming truncated bytes is the one
    /// state nothing recovers from.
    pub(crate) fn conditional_update(
        &self,
        new_bytes: &[u8],
        base: &Version,
    ) -> Result<UpdateOutcome, ProtocolError> {
        let new = base.next(Hash::of(new_bytes))?;
        let dir = self.dir();
        let stage = dir.join(stage_name(Uuid::new_v4()));
        match write_fresh(&stage, new_bytes, true) {
            Err(err) if is_missing(&err) => return Ok(UpdateOutcome::NotOnLatestVersion),
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
                    return Ok(UpdateOutcome::NotOnLatestVersion);
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
        let target = Marker { kind: MarkerKind::Latest, version: new.clone() };
        match self
            .hooks
            .rename(&dir.join(source.file_name()), &dir.join(target.file_name()))
        {
            Ok(()) => {
                self.hooks.at(Point::Renamed);
                if link_after_rename {
                    self.link_late(&stage, &content)?;
                }
                // Committed: from here an error means the commit may not be durable.
                fsync_dir(&dir)?;
                self.hooks.at(Point::BeforeTidyUp);
                self.tidy_up(&new, base);
                Ok(UpdateOutcome::Updated(new))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if self.hooks.sabotaged(Sabotage::TrustRenameEnoent) {
                    return Ok(UpdateOutcome::Updated(new));
                }
                self.after_failed_rename(base, new)
            }
            // The content file stays; the sweep owns it.
            Err(err) => Err(err.into()),
        }
    }

    /// The marker was not at our base, or our rename landed and its reply
    /// was lost: a listing tells them apart when it has caught up.
    fn after_failed_rename(
        &self,
        base: &Version,
        new: Version,
    ) -> Result<UpdateOutcome, ProtocolError> {
        let dir = self.dir();
        nudge(&dir);
        let markers = match self.list_markers()? {
            Listed::Markers(markers) => markers,
            Listed::NotFound => Vec::new(),
        };
        let ack_unproven = || self.hooks.sabotaged(Sabotage::AckUnproven);
        match Self::highest(&markers) {
            Some(marker) if marker.version == new => {
                self.hooks.observed(Observed::RenameLanded);
                fsync_dir(&dir)?;
                self.tidy_up(&new, base);
                Ok(UpdateOutcome::Updated(new))
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
                Ok(UpdateOutcome::NotOnLatestVersion)
            }
            // Superseded, whether or not our rename landed.
            Some(marker) if marker.version.n() > new.n() => {
                self.hooks.observed(Observed::RenameSuperseded);
                if ack_unproven() {
                    return Ok(UpdateOutcome::Updated(new));
                }
                self.unlink_version(&new);
                Ok(UpdateOutcome::NotOnLatestVersion)
            }
            // Marker names never recur, so our rename didn't land. The sweep
            // deletes our content once an unfreeze moves the marker past it.
            Some(marker)
                if marker.kind == MarkerKind::Frozen && marker.version == *base =>
            {
                self.hooks.observed(Observed::RenameFrozen);
                Err(ProtocolError::Frozen)
            }
            // Ambiguous: leave the content to the sweep. Spurious only after a
            // lost reply on NFSv3/4.0.
            _ => {
                self.hooks.observed(Observed::RenameUnproven);
                if ack_unproven() {
                    Ok(UpdateOutcome::Updated(new))
                } else {
                    Ok(UpdateOutcome::NotOnLatestVersion)
                }
            }
        }
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
                RepoDName::Marker(_) | RepoDName::Stage(_) => None,
            })
            .filter(|version| {
                is_dead(version, new) || (delete_above && version.n() > new.n())
            })
            .for_each(|version| self.unlink_version(&version));
    }

    /// The base's marker, or with [`Sabotage::RenameFromListedMarker`] the
    /// listing's highest `latest` marker.
    fn rename_source(&self, base: &Version) -> Marker {
        let base = Marker { kind: MarkerKind::Latest, version: base.clone() };
        if !self.hooks.sabotaged(Sabotage::RenameFromListedMarker) {
            return base;
        }
        nudge(&self.dir());
        match self.list_markers() {
            Ok(Listed::Markers(markers)) => markers
                .into_iter()
                .filter(|marker| marker.kind == MarkerKind::Latest)
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

    use super::{super::testing::*, UpdateOutcome::*, *};
    use crate::hooks::{NoHooks, Point, Sabotage};

    const V1: &[u8] = b"v1";
    const OTHER: &[u8] = b"other";

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
        let v1 = version(1, V1);
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), Updated(v1.clone()));
        assert_eq!(names(&repo_d(root.path())), current(&v1));
        assert_eq!(fs::read(repo_d(root.path()).join(v1.file_name())).unwrap(), V1);
        assert_eq!(hooks.reached(), ALL_POINTS);

        let v2 = version(2, V0);
        assert_eq!(update_plain(root.path(), V0, &v1), Updated(v2.clone()));
        assert_eq!(names(&repo_d(root.path())), current(&v2));
    }

    #[test]
    fn stale_base_conflicts_and_deletes_its_content() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v1 = version(1, V1);
        assert_eq!(update_plain(root.path(), V1, &v0), Updated(v1.clone()));
        assert_eq!(update_plain(root.path(), OTHER, &v0), NotOnLatestVersion);
        assert_eq!(names(&repo_d(root.path())), current(&v1));
    }

    #[test]
    fn loser_of_a_race_deletes_its_content() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let theirs = version(1, OTHER);
        let ours = version(1, V1);
        let (result, _) = with_paused(
            TestHooks::default(),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, V1, &v0),
            || {
                // Another writer's commit, without its tidy-up.
                fs::write(dir.join(theirs.file_name()), OTHER).unwrap();
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
        let theirs = version(1, OTHER);
        let (result, reached) = with_paused(
            TestHooks::default(),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, V1, &v0),
            || assert_eq!(update_plain(root.path(), OTHER, &v0), Updated(theirs.clone())),
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
        let v1 = version(1, V1);
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), Updated(v1.clone()));
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
            update_plain(&other_root, OTHER, &other_base);
            fs::rename(from, to)
        });
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), NotOnLatestVersion);
        assert_eq!(hooks.observed(), [Observed::RenameLostRace]);
    }

    #[test]
    fn lost_rename_reply_is_a_success() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let stale = version(0, OTHER);
        fs::write(dir.join(stale.file_name()), OTHER).unwrap();
        let hooks = TestHooks::default().rename(lose_rename_reply);
        let v1 = version(1, V1);
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), Updated(v1.clone()));
        // Tidied up.
        assert_eq!(names(&dir), current(&v1));
        assert_eq!(hooks.observed(), [Observed::RenameLanded]);
    }

    #[test]
    fn lost_rename_reply_then_freeze_is_a_success() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let v1 = version(1, V1);
        let freezing_dir = dir.clone();
        let frozen_v1 = frozen(&v1);
        let hooks = TestHooks::default().rename(move |from, to| {
            fs::rename(from, to)?;
            fs::rename(to, freezing_dir.join(&frozen_v1))?;
            Err(io::ErrorKind::NotFound.into())
        });
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), Updated(v1.clone()));
        assert_eq!(names(&dir), set([v1.file_name(), frozen(&v1)]));
    }

    #[test]
    fn frozen_base_is_reported_and_keeps_the_content() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let v1 = version(1, V1);
        let (result, _) = with_paused(
            TestHooks::default(),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, V1, &v0),
            || fs::rename(dir.join(latest(&v0)), dir.join(frozen(&v0))).unwrap(),
        );
        assert!(matches!(result, Err(ProtocolError::Frozen)), "{result:?}");
        assert_eq!(names(&dir), set([v0.file_name(), frozen(&v0), v1.file_name()]));
    }

    #[test]
    fn marker_two_ahead_supersedes_the_attempt() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let v2 = version(2, OTHER);
        let (result, _) = with_paused(
            TestHooks::default(),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, V1, &v0),
            || {
                fs::write(dir.join(v2.file_name()), OTHER).unwrap();
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
        let v1 = version(1, V1);
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), NotOnLatestVersion);
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
        let err = update(root.path(), &hooks, V1, &v0).unwrap_err();
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
        let err = update(root.path(), &hooks, V1, &v0).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Io(err) if err.to_string() == "server down")
        );
        let dir = repo_d(root.path());
        let v1 = version(1, V1);
        assert_eq!(names(&dir), set([v0.file_name(), latest(&v0), v1.file_name()]));
    }

    #[test]
    fn other_link_errors_remove_the_stage() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default().link(|_, _| Err(io::Error::other("no links")));
        let err = update(root.path(), &hooks, V1, &v0).unwrap_err();
        assert!(matches!(&err, ProtocolError::Io(err) if err.to_string() == "no links"));
        assert_eq!(names(&repo_d(root.path())), current(&v0));
    }

    #[test]
    fn missing_repo_d_conflicts_and_creates_nothing() {
        let root = tempfile::tempdir().unwrap();
        let v0 = version(0, V0);
        assert_eq!(update_plain(root.path(), V1, &v0), NotOnLatestVersion);
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
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), NotOnLatestVersion);
        assert_eq!(names(&dir), current(&v0));
    }

    #[test]
    fn same_bytes_as_the_base_are_a_new_version() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v1 = version(1, V0);
        assert_eq!(update_plain(root.path(), V0, &v0), Updated(v1.clone()));
        assert_eq!(names(&repo_d(root.path())), current(&v1));
    }

    #[test]
    fn tidy_up_deletes_dead_versions_and_keeps_those_above() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let above = version(5, OTHER);
        let next_attempt = version(2, OTHER);
        let stage = format!("stage.{}", Uuid::new_v4());
        for name in [
            version(0, OTHER).file_name(),
            version(1, OTHER).file_name(),
            above.file_name(),
            next_attempt.file_name(),
            stage.clone(),
            ".nfs123".to_string(),
        ] {
            fs::write(dir.join(name), b"").unwrap();
        }
        let v1 = version(1, V1);
        assert_eq!(update_plain(root.path(), V1, &v0), Updated(v1.clone()));
        let mut expected = current(&v1);
        expected.extend([above.file_name(), next_attempt.file_name(), stage]);
        expected.insert(".nfs123".to_string());
        assert_eq!(names(&dir), expected);
    }

    // ── Sabotages: each breaks the protocol in its own way ──────────────────

    #[test]
    fn sabotage_skip_link_commits_a_marker_without_bytes() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default().sabotage(Sabotage::SkipLink);
        let v1 = version(1, V1);
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), Updated(v1.clone()));
        assert_eq!(names(&repo_d(root.path())), set([latest(&v1)]));
    }

    #[test]
    fn sabotage_rename_from_listed_marker_loses_an_update() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let v1 = version(1, V1);
        assert_eq!(update_plain(root.path(), V1, &v0), Updated(v1.clone()));
        let hooks = TestHooks::default().sabotage(Sabotage::RenameFromListedMarker);
        let overwrite = version(1, OTHER);
        assert_eq!(
            update(root.path(), &hooks, OTHER, &v0).unwrap(),
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
        let v1 = version(1, V1);
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), Updated(v1.clone()));
        let dir = repo_d(root.path());
        assert_eq!(names(&dir), set([v0.file_name(), latest(&v0), v1.file_name()]));
    }

    #[test]
    fn sabotage_ack_unproven_acknowledges_ambiguous_failures() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks =
            TestHooks::default().rename(fail_rename).sabotage(Sabotage::AckUnproven);
        let v1 = version(1, V1);
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), Updated(v1.clone()));
        let dir = repo_d(root.path());
        assert_eq!(names(&dir), set([v0.file_name(), latest(&v0), v1.file_name()]));
    }

    #[test]
    fn sabotage_ack_unproven_acknowledges_superseded_attempts() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let v2 = version(2, OTHER);
        let (result, _) = with_paused(
            TestHooks::default().sabotage(Sabotage::AckUnproven),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, V1, &v0),
            || {
                fs::write(dir.join(v2.file_name()), OTHER).unwrap();
                fs::rename(dir.join(latest(&v0)), dir.join(latest(&v2))).unwrap();
            },
        );
        assert_eq!(result.unwrap(), Updated(version(1, V1)));
        assert!(dir.join(latest(&v2)).exists());
    }

    #[test]
    fn sabotage_tidy_up_deletes_current_leaves_a_marker_without_bytes() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let hooks = TestHooks::default().sabotage(Sabotage::TidyUpDeletesCurrent);
        let v1 = version(1, V1);
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), Updated(v1.clone()));
        assert_eq!(names(&repo_d(root.path())), set([latest(&v1)]));
    }

    #[test]
    fn sabotage_delete_above_marker_deletes_attempts_in_flight() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        fs::write(dir.join(version(2, OTHER).file_name()), OTHER).unwrap();
        let hooks = TestHooks::default().sabotage(Sabotage::DeleteAboveMarker);
        let v1 = version(1, V1);
        assert_eq!(update(root.path(), &hooks, V1, &v0).unwrap(), Updated(v1.clone()));
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
            |hooks| update(root.path(), hooks, V1, &v0),
            || {
                assert!(dir.join(latest(&v0)).exists());
                assert!(!dir.join(v0.file_name()).exists());
            },
        );
        assert_eq!(result.unwrap(), Updated(version(1, V1)));
    }

    #[test]
    fn sabotage_link_after_rename_commits_a_marker_before_its_bytes() {
        let root = tempfile::tempdir().unwrap();
        let v0 = seed(root.path());
        let dir = repo_d(root.path());
        let v1 = version(1, V1);
        let (result, reached) = with_paused(
            TestHooks::default().sabotage(Sabotage::LinkAfterRename),
            Point::Renamed,
            |hooks| update(root.path(), hooks, V1, &v0),
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
        let theirs = version(1, OTHER);
        let (result, _) = with_paused(
            TestHooks::default().sabotage(Sabotage::DeleteWinnerFile),
            Point::BeforeRename,
            |hooks| update(root.path(), hooks, V1, &v0),
            || assert_eq!(update_plain(root.path(), OTHER, &v0), Updated(theirs.clone())),
        );
        assert_eq!(result.unwrap(), NotOnLatestVersion);
        assert_eq!(names(&repo_d(root.path())), set([latest(&theirs)]));
    }
}
