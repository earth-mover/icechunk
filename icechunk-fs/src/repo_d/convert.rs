//! The administrative conversions between the standard layout and the
//! filesystem layout v1, see "Conversion from the standard layout" and
//! "Conversion to the standard layout" in design-docs/020-filesystem-storage.md.

use std::{fs, io, iter, thread};

use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use icechunk_format::{
    IcechunkFormatError, IcechunkResult, OVERWRITTEN_FILES_PATH,
    layout_v1::{self, FeatureChange},
    repo_info::{DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE, RepoInfo, WritableShapeMode},
    repo_info_file,
    writer_features::{min_version, missing_features},
};
use uuid::Uuid;

use super::{
    FIRST_BACKOFF, OPS_LOG_FILES, ProtocolError, RepoD, TIME_SCALE,
    commit::UpdateOutcome,
    discard_tmp_dir,
    lifecycle::{FEATURES, RootRepo, unix_millis, write_marker},
    mirror::{ForkCheck, PublishOutcome, REREADS, is_gone, is_stale, with_root_lock},
    read::{Current, read_version_file},
};
use crate::{
    hooks::{ConversionStep, FromStandardStep, Point, Sabotage, ToStandardStep},
    names::{Hash, REPO, RootName, Version, parse_root_name, tmp_dir_name},
    nudge::nudge,
    path::key_path,
    read::read_all,
    storage::is_missing,
    write::{CreateOutcome as Written, create_exclusive, fsync_dir, write_fresh},
};

const RERUN: &str = "rerun convert_to_filesystem_layout() to complete the conversion";
const TO_STANDARD_RERUN: &str =
    "rerun convert_to_standard_layout() to complete the conversion";
const STANDARD: &str = "the standard repo info";
/// The relistings of `repo.d` of the conversion to the standard layout
/// while the fork check, or the change, shows that a listing was stale.
const RELISTS: usize = 8;

/// What [`FilesystemStorage::convert_to_filesystem_layout`] or
/// [`FilesystemStorage::convert_to_standard_layout`] did.
///
/// [`FilesystemStorage::convert_to_filesystem_layout`]: crate::FilesystemStorage::convert_to_filesystem_layout
/// [`FilesystemStorage::convert_to_standard_layout`]: crate::FilesystemStorage::convert_to_standard_layout
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionOutcome {
    /// The repository was converted, by this call or by an interrupted run
    /// that this call completed.
    Converted,
    /// The repository was in the target layout already. For the conversion to
    /// the standard layout, that includes an interrupted run that had renamed
    /// `repo.d` aside: this call only deleted what it left.
    NothingToConvert,
}

/// `$ROOT/repo` next to `repo.d`, as a rerun of the conversion reads it.
#[derive(Debug, PartialEq, Eq)]
enum Mirror {
    Missing,
    Decoded {
        bytes: Bytes,
        lists_feature: bool,
    },
    /// It can't be read or decoded, after re-reads.
    Unreadable,
}

impl RepoD<'_> {
    /// `FilesystemStorage::convert_to_filesystem_layout`. Administrative:
    /// nothing else may use the repository. A rerun after a crash decides from
    /// the state it finds. `None` if there is no repository.
    pub(crate) fn convert_from_standard(
        &self,
    ) -> Result<Option<ConversionOutcome>, ProtocolError> {
        let mut classified_again = false;
        let outcome = loop {
            nudge(self.root);
            if self.repo_d_exists()? {
                break self.resume_conversion()?;
            }
            let standard = match self.root_repo()? {
                RootRepo::Standard(bytes) => bytes,
                RootRepo::Missing | RootRepo::InterruptedCreation => return Ok(None),
                RootRepo::OrphanMirror => {
                    return Err(ProtocolError::orphan_mirror(self.root));
                }
                RootRepo::Undecodable(why) => {
                    return Err(ProtocolError::repo_unreadable(self.root, &why));
                }
            };
            match self.convert_standard(&standard)? {
                Some(outcome) => break outcome,
                None if !classified_again => classified_again = true,
                None => {
                    return Err(ProtocolError::Other(format!(
                        "renaming the new repo.d into {} failed twice; {RERUN}",
                        self.root.display()
                    )));
                }
            }
        };
        self.discard_tmp_dirs().map_err(|err| {
            interrupted("deleting the leftover repo.d.tmp.* directories", err)
        })?;
        Ok(Some(outcome))
    }

    /// Steps 2 to 7 on the standard repo info `standard`. `None` if the
    /// directory rename found a `repo.d` or its directory gone: classify again.
    fn convert_standard(
        &self,
        standard: &[u8],
    ) -> Result<Option<ConversionOutcome>, ProtocolError> {
        let (num_updates, now) = check_convertible(standard, STANDARD)?;
        let backup = repo_info_file::new_backup_path();
        self.write_backup(&backup, standard).map_err(|err| {
            interrupted("writing the backup of the standard repo info", err)
        })?;
        self.hooks.at(from_standard(FromStandardStep::Backup));

        let bytes =
            layout_v1::convert(standard, FeatureChange::Add, &backup, now, num_updates)
                .map_err(|err| unconvertible(STANDARD, &err))?;
        let building =
            |err: io::Error| interrupted("building the new repo.d", err.into());
        let initial = unix_millis()
            .and_then(|n| Version::initial(n, Hash::of(&bytes)))
            .map_err(building)?;
        let tmp = self.root.join(tmp_dir_name(Uuid::new_v4()));
        fs::DirBuilder::new().create(&tmp).map_err(building)?;
        let built = write_fresh(&tmp.join(initial.file_name()), &bytes, true)
            .and_then(|()| write_marker(&tmp, &initial));
        if let Err(err) = built {
            self.discard(&tmp);
            return Err(building(err));
        }
        self.hooks.at(from_standard(FromStandardStep::Build));

        let renaming = |err: io::Error| {
            interrupted("renaming the new repo.d into place", err.into())
        };
        match self.hooks.rename(&tmp, &self.dir()) {
            Ok(()) => fsync_dir(self.root).map_err(renaming)?,
            // A `repo.d` that step 1's listing missed.
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty
                ) =>
            {
                self.discard(&tmp);
                return Ok(None);
            }
            // Our rename landed and its reply was lost, or the directory was
            // swept: the provenance check decides.
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                self.discard(&tmp);
                return Err(renaming(err));
            }
        }
        self.hooks.at(from_standard(FromStandardStep::RenameDir));
        self.publish_conversion(&initial, &bytes, Some(Hash::of(standard)))?;
        Ok(Some(ConversionOutcome::Converted))
    }

    /// Step 1 with `repo.d`: what an earlier run left, or a finished
    /// conversion.
    fn resume_conversion(&self) -> Result<ConversionOutcome, ProtocolError> {
        let dir = self.dir();
        let Some(Current { bytes, version }) = self.read_latest()? else {
            return Err(ProtocolError::Other(format!(
                "{} disappeared during the conversion; {RERUN}",
                dir.display()
            )));
        };
        let describe = || {
            format!("the current version {} of {}", version.file_name(), dir.display())
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
        match self.read_mirror()? {
            Mirror::Decoded { lists_feature: true, .. } => {
                Ok(ConversionOutcome::NothingToConvert)
            }
            Mirror::Missing => {
                self.publish_conversion(&version, &bytes, None)?;
                Ok(ConversionOutcome::NothingToConvert)
            }
            Mirror::Unreadable => Err(ProtocolError::MirrorUnreadable(format!(
                "{} couldn't be read or decoded; nothing was changed",
                mirror.display()
            ))),
            Mirror::Decoded { bytes: standard, lists_feature: false } => {
                if !self.hooks.sabotaged(Sabotage::FromStandardWithoutProvenance)
                    && !self.built_from(&bytes, &standard)?
                {
                    return Err(ProtocolError::manual_recovery(
                        self.root,
                        &format!(
                            "{dir} exists next to {mirror}, which doesn't list {feature}, and no \
                         backup named or linked in the last {OPS_LOG_FILES} ops-log files of \
                         {current} holds the bytes of {mirror}: convert_to_filesystem_layout() \
                         didn't build {dir} from it, {mirror} changed after the conversion \
                         renamed {dir} into place (a fork), or more changes landed since the \
                         conversion than those files hold. Nothing was changed. To keep \
                         {dir}, move {mirror} aside and run publish_mirror(); to keep \
                         {mirror}, archive and delete {dir}, then run \
                         convert_to_filesystem_layout()",
                            dir = dir.display(),
                            mirror = mirror.display(),
                            feature = layout_v1::FEATURE,
                            current = describe(),
                        ),
                    ));
                }
                self.publish_conversion(&version, &bytes, Some(Hash::of(&standard)))?;
                Ok(ConversionOutcome::Converted)
            }
        }
    }

    /// Step 3: `bytes` at `overwritten/<backup>`, create-only and fsynced.
    fn write_backup(&self, backup: &str, bytes: &[u8]) -> Result<(), ProtocolError> {
        let path = self.root.join(OVERWRITTEN_FILES_PATH).join(backup);
        match create_exclusive(&path, bytes, true)? {
            Written::Created => Ok(()),
            Written::Exists => Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("the new backup {} exists already", path.display()),
            )
            .into()),
        }
    }

    /// Step 6, the provenance check: whether a backup named or linked in the
    /// ops log of `current` holds `standard`. Reads at most [`OPS_LOG_FILES`] files of
    /// the ops log, newest first: `current`, then the backups that each
    /// file's `repo_before_updates` names.
    fn built_from(&self, current: &[u8], standard: &[u8]) -> Result<bool, ProtocolError> {
        let mut file = Bytes::copy_from_slice(current);
        for read in 1..=OPS_LOG_FILES {
            let named = layout_v1::backup_paths(&file).map_err(|err| {
                ProtocolError::Corrupt(format!(
                    "an ops-log file of {} doesn't decode: {err}",
                    self.dir().display()
                ))
            })?;
            for backup in &named.entries {
                if self
                    .read_backup(backup, standard.len())?
                    .is_some_and(|b| b == standard)
                {
                    return Ok(true);
                }
            }
            let Some(before) = named.repo_before_updates.filter(|_| read < OPS_LOG_FILES)
            else {
                return Ok(false);
            };
            match self.read_backup(&before, None)? {
                Some(bytes) if bytes == standard => return Ok(true),
                Some(bytes) => file = bytes,
                None => return Ok(false),
            }
        }
        Ok(false)
    }

    /// The backup `overwritten/<backup>`, `None` if it is missing or, given
    /// `len`, of another length.
    pub(super) fn read_backup(
        &self,
        backup: &str,
        len: impl Into<Option<usize>>,
    ) -> Result<Option<Bytes>, ProtocolError> {
        let path = key_path(self.root, &format!("{OVERWRITTEN_FILES_PATH}/{backup}"))
            .map_err(|err| {
                ProtocolError::Corrupt(format!(
                    "the ops log of {} names an invalid backup {backup:?}: {err}",
                    self.dir().display()
                ))
            })?;
        if let Some(len) = len.into() {
            match fs::metadata(&path) {
                Ok(metadata) if metadata.len() != len as u64 => return Ok(None),
                Ok(_) => {}
                Err(err) if is_missing(&err) => return Ok(None),
                Err(err) => return Err(err.into()),
            }
        }
        match read_all(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if is_missing(&err) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Step 7: publishes `version`, which a server-decided step proved
    /// current or nothing else can have changed, with the hash of the
    /// `standard` repo info it converted as known: no fork.
    fn publish_conversion(
        &self,
        version: &Version,
        bytes: &[u8],
        standard: Option<Hash>,
    ) -> Result<(), ProtocolError> {
        let known: Vec<Hash> =
            iter::once(version.hash().clone()).chain(standard).collect();
        let mirror = self.root.join(REPO);
        let mirror = mirror.display();
        match self.publish(version, Some(bytes), &known) {
            Ok(PublishOutcome::Current) => {}
            Ok(PublishOutcome::GaveUp { .. }) => {
                return Err(ProtocolError::Other(format!(
                    "the publication of the mirror {mirror} gave up; {RERUN}"
                )));
            }
            Err(ProtocolError::Forked(_)) => {
                return Err(ProtocolError::Forked(format!(
                    "{mirror} doesn't list {} and is neither a version file of {} nor the \
                     standard repo info this conversion converts; nothing was published",
                    layout_v1::FEATURE,
                    self.dir().display()
                )));
            }
            Err(err @ ProtocolError::MirrorUnreadable(_)) => {
                return Err(err.context("; nothing was published"));
            }
            Err(ProtocolError::Io(err)) => {
                return Err(ProtocolError::Other(format!(
                    "publishing the mirror {mirror} failed: {err}; {RERUN}"
                )));
            }
            Err(err) => return Err(err),
        }
        self.hooks.at(from_standard(FromStandardStep::Publish));
        Ok(())
    }

    /// `$ROOT/repo`, read again up to [`REREADS`] times with backoff while it
    /// can't be read or decoded.
    fn read_mirror(&self) -> Result<Mirror, ProtocolError> {
        let path = self.root.join(REPO);
        let mut pauses = iter::successors(Some(FIRST_BACKOFF), |pause| Some(*pause * 2));
        for read in 0..=REREADS {
            if read > 0
                && let Some(pause) = pauses.next()
            {
                thread::sleep(pause / TIME_SCALE);
            }
            nudge(self.root);
            match with_root_lock(self.root, || read_all(&path)) {
                Ok(bytes) => match layout_v1::lists_feature(&bytes) {
                    Ok(lists_feature) => {
                        return Ok(Mirror::Decoded { bytes, lists_feature });
                    }
                    Err(err) => {
                        tracing::debug!(%err, path = %path.display(), "could not decode the mirror");
                    }
                },
                Err(err) if is_missing(&err) => return Ok(Mirror::Missing),
                Err(err) if is_stale(&err) || self.repo_is_not_a_file() => {
                    tracing::debug!(%err, path = %path.display(), "could not read the mirror");
                }
                Err(err) => return Err(err.into()),
            }
        }
        Ok(Mirror::Unreadable)
    }

    /// Step 8: discards every `repo.d.tmp.*`. Nothing else runs, so they are
    /// all leftovers.
    fn discard_tmp_dirs(&self) -> Result<(), ProtocolError> {
        let tmp_dirs: Vec<_> = fs::read_dir(self.root)?
            .filter_map(|entry| {
                entry
                    .map(|entry| {
                        let name = entry.file_name();
                        (name.to_str().and_then(parse_root_name)
                            == Some(RootName::TmpDir))
                        .then(|| entry.path())
                    })
                    .transpose()
            })
            .collect::<io::Result<_>>()?;
        for tmp in tmp_dirs {
            match discard_tmp_dir(self.root, &tmp) {
                Err(err) if !is_missing(&err) => return Err(err.into()),
                _ => {}
            }
        }
        Ok(())
    }
}

/// The version without the feature that the conversion to the standard layout
/// publishes.
struct Featureless {
    version: Version,
    bytes: Bytes,
    /// The version this run's change replaced; `None` on a rerun that found the
    /// change made.
    replaced: Option<Version>,
}

impl RepoD<'_> {
    /// `FilesystemStorage::convert_to_standard_layout`. Administrative:
    /// nothing else may use the repository. A rerun after a crash decides from
    /// the state it finds. `None` if there is no repository.
    pub(crate) fn convert_to_standard(
        &self,
    ) -> Result<Option<ConversionOutcome>, ProtocolError> {
        let mut classified_again = false;
        let outcome = loop {
            nudge(self.root);
            if !self.repo_d_exists()? {
                match self.root_repo()? {
                    // An earlier run renamed `repo.d` aside, or the repository
                    // is in the standard layout already.
                    RootRepo::Standard(_) => break ConversionOutcome::NothingToConvert,
                    RootRepo::Missing | RootRepo::InterruptedCreation => return Ok(None),
                    RootRepo::OrphanMirror => {
                        return Err(ProtocolError::orphan_mirror(self.root));
                    }
                    RootRepo::Undecodable(why) => {
                        return Err(ProtocolError::repo_unreadable(self.root, &why));
                    }
                }
            }
            match self.convert_repo_d()? {
                Some(()) => break ConversionOutcome::Converted,
                None if !classified_again => classified_again = true,
                None => {
                    return Err(ProtocolError::Other(format!(
                        "{} was listed and then missing twice; {TO_STANDARD_RERUN}",
                        self.dir().display()
                    )));
                }
            }
        };
        self.delete_tmp_dirs()?;
        Ok(Some(outcome))
    }

    /// Steps 2 to 6. `None` if `repo.d` turned out to be gone: classify again.
    fn convert_repo_d(&self) -> Result<Option<()>, ProtocolError> {
        let Some(featureless) = self.change_to_standard()? else {
            return Ok(None);
        };
        if self.hooks.sabotaged(Sabotage::DeleteDirBeforePublish)
            && self.rename_aside()?
        {
            self.hooks.at(to_standard(ToStandardStep::RenameDir));
            self.delete_tmp_dirs()?;
        }
        self.publish_to_standard(&featureless)?;
        self.hooks.at(to_standard(ToStandardStep::Publish));
        if !self.rename_aside()? {
            return Ok(None);
        }
        self.hooks.at(to_standard(ToStandardStep::RenameDir));
        Ok(Some(()))
    }

    /// Steps 2 to 4: the current version without the feature, made by this
    /// run's change or found made by an earlier run's. `None` if `repo.d`
    /// doesn't exist.
    fn change_to_standard(&self) -> Result<Option<Featureless>, ProtocolError> {
        let dir = self.dir();
        let mirror = self.root.join(REPO);
        let (mut relists, mut conflicts) = (0, 0);
        loop {
            let Some(Current { bytes, version }) = self.read_latest()? else {
                return Ok(None);
            };
            let current = format!(
                "the current version {} of {}",
                version.file_name(),
                dir.display()
            );
            match layout_v1::lists_feature(&bytes) {
                // An earlier run made the change.
                Ok(false) => {
                    return Ok(Some(Featureless { version, bytes, replaced: None }));
                }
                Ok(true) => {}
                Err(err) => {
                    return Err(ProtocolError::Corrupt(format!(
                        "{current} doesn't decode: {err}"
                    )));
                }
            }
            let (num_updates, now) = check_convertible(&bytes, &current)?;

            self.hooks.at(to_standard(ToStandardStep::ForkCheck));
            match self.fork_check(&[version.hash().clone()])? {
                ForkCheck::Fork => {
                    return Err(ProtocolError::Forked(format!(
                        "{} doesn't list {} and is neither a version file of {} nor {current}; \
                         nothing was changed",
                        mirror.display(),
                        layout_v1::FEATURE,
                        dir.display()
                    )));
                }
                ForkCheck::Unreadable => {
                    return Err(ProtocolError::MirrorUnreadable(format!(
                        "{} couldn't be read or decoded; nothing was changed",
                        mirror.display()
                    )));
                }
                // The listing was stale: an earlier run's change and
                // publication landed.
                ForkCheck::NotFork { copy_of: Some(copy) }
                    if self.copies_a_later_change(&copy, &version)? =>
                {
                    if relists == RELISTS {
                        return Err(ProtocolError::Other(format!(
                            "{} holds version {} of {}, which is newer than {current} or \
                             doesn't list {}, but {RELISTS} more listings still showed \
                             {current}: listings of {} are stale. Nothing was changed; run \
                             convert_to_standard_layout() again once they refresh",
                            mirror.display(),
                            copy.file_name(),
                            dir.display(),
                            layout_v1::FEATURE,
                            dir.display()
                        )));
                    }
                    relists += 1;
                    nudge(&dir);
                    continue;
                }
                ForkCheck::NotFork { .. } => {}
            }

            let backup = repo_info_file::new_backup_path();
            self.write_backup(&backup, &bytes).map_err(|err| {
                to_standard_interrupted("writing the backup of the current version", err)
            })?;
            let converted = layout_v1::convert(
                &bytes,
                FeatureChange::Remove,
                &backup,
                now,
                num_updates,
            )
            .map_err(|err| unconvertible(&current, &err))?;
            let outcome = if self.hooks.sabotaged(Sabotage::ToStandardWithoutChange) {
                UpdateOutcome::Updated(version.next(Hash::of(&converted))?)
            } else {
                self.attempt_change(&converted, &version)
                    .map_err(|err| {
                        to_standard_interrupted(
                            "the change to the version without the feature",
                            err,
                        )
                    })?
                    .outcome
            };
            match outcome {
                UpdateOutcome::Updated(new) => {
                    self.hooks.at(to_standard(ToStandardStep::Change));
                    return Ok(Some(Featureless {
                        version: new,
                        bytes: Bytes::from(converted),
                        replaced: Some(version),
                    }));
                }
                // Nothing else runs: the listing was stale.
                UpdateOutcome::NotOnLatestVersion if conflicts < RELISTS => {
                    conflicts += 1;
                    nudge(&dir);
                }
                UpdateOutcome::NotOnLatestVersion => {
                    return Err(ProtocolError::Other(format!(
                        "the change from {current} conflicted after {RELISTS} more listings \
                         of {}: they are stale, or something else changes the repository. \
                         Nothing was changed; with nothing else using the repository, run \
                         convert_to_standard_layout() again",
                        dir.display()
                    )));
                }
            }
        }
    }

    /// Whether `$ROOT/repo`, a copy of the version file `copy`, shows that a
    /// listing showing `read` as current was stale: `copy` is newer, or
    /// doesn't list the feature.
    fn copies_a_later_change(
        &self,
        copy: &Version,
        read: &Version,
    ) -> Result<bool, ProtocolError> {
        if copy.n() > read.n() {
            return Ok(true);
        }
        if copy == read {
            return Ok(false);
        }
        match read_version_file(&self.dir(), copy) {
            Ok(bytes) => Ok(!layout_v1::lists_feature(&bytes).map_err(|err| {
                ProtocolError::Corrupt(format!(
                    "{} in {} doesn't decode: {err}",
                    copy.file_name(),
                    self.dir().display()
                ))
            })?),
            // A dead version, deleted since the fork check listed it.
            Err(err) if is_gone(&err) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Step 5: publishes the version without the feature, which a
    /// server-decided step proved current, with the version it replaced on a
    /// first run as known.
    fn publish_to_standard(
        &self,
        featureless: &Featureless,
    ) -> Result<(), ProtocolError> {
        let Featureless { version, bytes, replaced } = featureless;
        let known: Vec<Hash> =
            iter::once(version).chain(replaced).map(|v| v.hash().clone()).collect();
        let mirror = self.root.join(REPO);
        let mirror = mirror.display();
        let dir = self.dir();
        let dir = dir.display();
        match self.publish(version, Some(bytes), &known) {
            Ok(PublishOutcome::Current) => Ok(()),
            Ok(PublishOutcome::GaveUp { .. }) => Err(ProtocolError::Other(format!(
                "the publication of the mirror {mirror} gave up; {TO_STANDARD_RERUN}"
            ))),
            Err(ProtocolError::Forked(_)) => Err(ProtocolError::Forked(format!(
                "{mirror} doesn't list {} and is neither a version file of {dir} nor a \
                 version this conversion read; the conversion's change landed, nothing \
                 was published and {dir} was kept. Recover the fork by hand (see the \
                 storage guide's section on forks), then {TO_STANDARD_RERUN}, not \
                 convert_to_filesystem_layout()",
                layout_v1::FEATURE,
            ))),
            Err(ProtocolError::MirrorUnreadable(_)) => {
                Err(ProtocolError::MirrorUnreadable(format!(
                    "{mirror} couldn't be read or decoded; the conversion's change landed, \
                 nothing was published and {dir} was kept. Make {mirror} readable or \
                 move it aside, then {TO_STANDARD_RERUN}"
                )))
            }
            Err(ProtocolError::Io(err)) => Err(ProtocolError::Other(format!(
                "publishing the mirror {mirror} failed: {err}; {TO_STANDARD_RERUN}"
            ))),
            Err(err) => Err(err),
        }
    }

    /// Step 6: renames `repo.d` to a fresh `repo.d.tmp.<id>`. `false` if
    /// `repo.d` was gone and the rename wasn't ours.
    fn rename_aside(&self) -> Result<bool, ProtocolError> {
        let aside = self.root.join(tmp_dir_name(Uuid::new_v4()));
        let renaming =
            |err: io::Error| to_standard_interrupted("renaming repo.d aside", err.into());
        match self.hooks.rename(&self.dir(), &aside) {
            Ok(()) => {}
            // Ours after a lost reply, if `aside` exists.
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                match fs::symlink_metadata(&aside) {
                    Ok(_) => {}
                    Err(err) if is_missing(&err) => return Ok(false),
                    Err(err) => return Err(renaming(err)),
                }
            }
            Err(err) => return Err(renaming(err)),
        }
        fsync_dir(self.root).map_err(renaming)?;
        Ok(true)
    }

    /// Step 7: deletes every `repo.d.tmp.*`, then fsyncs the root.
    fn delete_tmp_dirs(&self) -> Result<(), ProtocolError> {
        let deleting =
            |err| to_standard_interrupted("deleting the repo.d.tmp.* directories", err);
        self.discard_tmp_dirs().map_err(deleting)?;
        fsync_dir(self.root).map_err(|err| deleting(err.into()))?;
        self.hooks.at(to_standard(ToStandardStep::DeleteDir));
        Ok(())
    }
}

/// Check 1 for every listed feature but `filesystem-layout-v1`, and the
/// shape check, of the repo info `what` that a conversion converts, before
/// anything is written. Returns the repository's own page size and the time
/// of the conversion's update.
fn check_convertible(
    bytes: &[u8],
    what: &str,
) -> Result<(u16, DateTime<Utc>), ProtocolError> {
    let unconvertible = |err| unconvertible(what, &err);
    let info = repo_info_file::decode(bytes).map_err(unconvertible)?;
    let listed = info.writer_features().map_err(unconvertible)?;
    let missing = missing_features(&listed, FEATURES);
    if let Some(min_version) = min_version(&missing) {
        return Err(ProtocolError::MissingFeatures { missing, min_version });
    }
    info.check_writable_shape(WritableShapeMode::AnyChange).map_err(unconvertible)?;
    let num_updates = info
        .num_updates_per_repo_info_file()
        .map_err(unconvertible)?
        .unwrap_or(DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE);
    Ok((num_updates, update_time(&info).map_err(unconvertible)?))
}

fn unconvertible(what: &str, err: &IcechunkFormatError) -> ProtocolError {
    ProtocolError::Other(format!("{what} can't be converted: {}", err.kind()))
}

/// Now, to the microsecond the ops log stores, raised above the time of the
/// newest entry if the clock lags: the format requires a later time.
fn update_time(info: &RepoInfo) -> IcechunkResult<DateTime<Utc>> {
    let now = Utc::now();
    let now = DateTime::from_timestamp_micros(now.timestamp_micros()).unwrap_or(now);
    Ok(match info.latest_updates()?.next().transpose()? {
        Some((_, newest, _)) if newest >= now => newest + TimeDelta::microseconds(1),
        _ => now,
    })
}

/// An I/O failure of a step of the conversion from the standard layout, as
/// an error naming the conversion to rerun.
fn interrupted(step: &str, err: ProtocolError) -> ProtocolError {
    incomplete(RERUN, step, err)
}

/// [`interrupted`], for the conversion to the standard layout.
fn to_standard_interrupted(step: &str, err: ProtocolError) -> ProtocolError {
    incomplete(TO_STANDARD_RERUN, step, err)
}

fn incomplete(rerun: &str, step: &str, err: ProtocolError) -> ProtocolError {
    match err {
        ProtocolError::Io(err) => {
            ProtocolError::Other(format!("{step} failed: {err}; {rerun}"))
        }
        err => err,
    }
}

fn from_standard(step: FromStandardStep) -> Point {
    Point::Conversion(ConversionStep::FromStandard(step))
}

fn to_standard(step: ToStandardStep) -> Point {
    Point::Conversion(ConversionStep::ToStandard(step))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        path::{Path, PathBuf},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::Duration,
    };

    use icechunk_format::{
        SnapshotId,
        format_constants::SpecVersionBin,
        repo_info::{RepoAvailability, RepoStatus, UpdateType},
        snapshot::SnapshotInfo,
    };

    use super::{super::testing::*, *};
    use crate::{
        hooks::{NoHooks, Observed, ProtocolHooks},
        names::{RepoDName, parse_repo_d_name},
        repo_d::lifecycle::OpenStatus,
    };

    fn repo(root: &Path) -> RepoD<'_> {
        RepoD { root, hooks: &NoHooks }
    }

    // ── convert_from_standard ───────────────────────────────────────────────

    const STEPS: [FromStandardStep; 4] = [
        FromStandardStep::Backup,
        FromStandardStep::Build,
        FromStandardStep::RenameDir,
        FromStandardStep::Publish,
    ];

    fn at_step(step: FromStandardStep) -> Point {
        Point::Conversion(ConversionStep::FromStandard(step))
    }

    /// A repo info in the standard layout. With `num_updates`, its stored
    /// configuration sets `num_updates_per_repo_info_file`.
    fn standard(num_updates: Option<u16>) -> Vec<u8> {
        let time = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let config =
            num_updates.map(|n| BTreeMap::from([("num_updates_per_repo_info_file", n)]));
        let info = RepoInfo::initial(
            SpecVersionBin::current(),
            SnapshotInfo {
                id: SnapshotId::new([1; 12]),
                parent_id: None,
                flushed_at: time,
                message: "initial".to_string(),
                metadata: Default::default(),
                pruned_ancestor_tx_logs: vec![],
            },
            DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE,
            config.as_ref(),
            Some(time),
            &[],
        );
        repo_info_file::encode(&info, SpecVersionBin::current(), 3).unwrap()
    }

    fn convert(
        root: &Path,
        hooks: &dyn ProtocolHooks,
    ) -> Result<Option<ConversionOutcome>, ProtocolError> {
        RepoD { root, hooks }.convert_from_standard()
    }

    fn converted(root: &Path) -> ConversionOutcome {
        convert(root, &NoHooks).unwrap().unwrap()
    }

    fn backup(root: &Path, name: &str) -> Vec<u8> {
        fs::read(root.join("overwritten").join(name)).unwrap()
    }

    fn backups(root: &Path) -> BTreeSet<String> {
        let dir = root.join("overwritten");
        if dir.exists() { names(&dir) } else { BTreeSet::new() }
    }

    fn tmp_dirs(root: &Path) -> Vec<String> {
        names(root).into_iter().filter(|name| name.starts_with("repo.d.tmp.")).collect()
    }

    /// The end state of a conversion of `standard`: `repo.d` holding its first
    /// version and marker, that version being `standard` with the feature, the
    /// gate and one new `RepoStatusChangedUpdate` whose predecessor names a
    /// backup of `standard`, `$ROOT/repo` its copy, no `repo.d.tmp.*`.
    fn assert_converted(root: &Path, standard: &[u8]) -> Version {
        let Current { bytes, version } = repo(root).read_latest().unwrap().unwrap();
        assert_eq!(
            names(&root.join("repo.d")),
            set([version.file_name(), latest(&version)])
        );
        assert_eq!(fs::read(root.join("repo")).unwrap(), bytes);
        assert_eq!(tmp_dirs(root), Vec::<String>::new());

        let before = repo_info_file::decode(standard).unwrap();
        let after = repo_info_file::decode(&bytes).unwrap();
        assert!(layout_v1::lists_feature(&bytes).unwrap());
        // The gate, as stored.
        let gate = serde_json::to_value(after.raw_status().unwrap()).unwrap();
        assert_eq!(gate["availability"], "ReadOnly", "{gate}");
        assert!(gate.get("underlying").is_some(), "{gate}");
        assert_eq!(after.status().unwrap(), before.status().unwrap());
        let entries: Vec<_> =
            after.latest_updates().unwrap().map(Result::unwrap).collect();
        // Paged out to the backup otherwise.
        if after.repo_before_updates().unwrap().is_none() {
            assert_eq!(entries.len(), before.latest_updates().unwrap().count() + 1);
        }
        assert!(
            matches!(entries[0].0, UpdateType::RepoStatusChangedUpdate { .. }),
            "{entries:?}"
        );
        let attempt = layout_v1::attempt_id(&bytes).unwrap().unwrap();
        assert_eq!(backup(root, &attempt), standard);
        version
    }

    #[test]
    fn convert_converts_a_standard_repo() {
        let root = tempfile::tempdir().unwrap();
        let standard = standard(None);
        plant(root.path(), &standard);
        let hooks = TestHooks::default();
        let before = now_ms();
        assert_eq!(
            convert(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        let after = now_ms();
        let v0 = assert_converted(root.path(), &standard);
        assert!(
            (before..=after).contains(&v0.n()),
            "{} not in {before}..={after}",
            v0.n()
        );
        assert_eq!(backups(root.path()).len(), 1);
        assert_eq!(
            hooks.reached(),
            [
                at_step(FromStandardStep::Backup),
                at_step(FromStandardStep::Build),
                at_step(FromStandardStep::RenameDir),
                Point::BeforePublish,
                Point::MirrorStaged,
                Point::BeforeMirrorRead,
                Point::BeforeMirrorRename,
                Point::MirrorRenamed,
                at_step(FromStandardStep::Publish),
            ]
        );
        assert_eq!(repo(root.path()).open_status().unwrap(), OpenStatus::Ok);

        // Done: nothing changes.
        let before = (names(root.path()), names(&root.path().join("repo.d")));
        assert_eq!(converted(root.path()), ConversionOutcome::NothingToConvert);
        assert_eq!((names(root.path()), names(&root.path().join("repo.d"))), before);
    }

    #[test]
    fn convert_keeps_the_repositorys_own_status() {
        let root = tempfile::tempdir().unwrap();
        let read_only = RepoStatus {
            availability: RepoAvailability::ReadOnly,
            set_at: DateTime::from_timestamp(1_700_000_001, 0).unwrap(),
            limited_availability_reason: Some("copying".to_string()),
        };
        let info = repo_info_file::decode(&standard(None))
            .unwrap()
            .set_status(SpecVersionBin::current(), &read_only, Some(("repo.s", 10)))
            .unwrap();
        let standard =
            repo_info_file::encode(&info, SpecVersionBin::current(), 3).unwrap();
        plant(root.path(), &standard);
        assert_eq!(converted(root.path()), ConversionOutcome::Converted);
        assert_converted(root.path(), &standard);
        let bytes = fs::read(root.path().join("repo")).unwrap();
        assert_eq!(repo_info_file::decode(&bytes).unwrap().status().unwrap(), read_only);
    }

    /// The ops log is paged at the repository's own configured size: with
    /// one update per file, the backup is named by `repo_before_updates`.
    #[test]
    fn convert_pages_at_the_repositorys_own_size() {
        for (num_updates, paged) in [(None, false), (Some(1), true)] {
            let root = tempfile::tempdir().unwrap();
            let standard = standard(num_updates);
            plant(root.path(), &standard);
            assert_eq!(converted(root.path()), ConversionOutcome::Converted);
            assert_converted(root.path(), &standard);
            let named =
                layout_v1::backup_paths(&fs::read(root.path().join("repo")).unwrap())
                    .unwrap();
            assert_eq!(named.repo_before_updates.is_some(), paged, "{num_updates:?}");
            assert_eq!(named.entries.is_empty(), paged, "{num_updates:?}");
        }
    }

    #[test]
    fn convert_without_a_standard_repo() {
        // Nothing, or an interrupted creation: no repository.
        for planted in [None, Some(repo_info(1))] {
            let root = tempfile::tempdir().unwrap();
            if let Some(bytes) = &planted {
                plant(root.path(), bytes);
            }
            assert_eq!(convert(root.path(), &NoHooks).unwrap(), None);
            assert_eq!(fs::read(root.path().join("repo")).ok(), planted);
            assert!(!root.path().join("repo.d").exists());
        }
        let root = tempfile::tempdir().unwrap();
        assert_eq!(convert(&root.path().join("missing"), &NoHooks).unwrap(), None);

        // A created repository.
        create(root.path(), &NoHooks, &v0_bytes());
        assert_eq!(converted(root.path()), ConversionOutcome::NothingToConvert);

        let refused = |planted: &[u8]| {
            let root = tempfile::tempdir().unwrap();
            plant(root.path(), planted);
            let err = convert(root.path(), &NoHooks).unwrap_err();
            assert_eq!(names(root.path()), strings(["repo"]));
            err
        };
        let err = refused(&changed_repo_info(1));
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("an orphan mirror")),
            "{err:?}"
        );
        let err = refused(UNDECODABLE);
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("can't be read or decoded and")),
            "{err:?}"
        );
    }

    /// Runs a conversion that crashes at `step`.
    fn crash_at(root: &Path, step: FromStandardStep) {
        let hooks = TestHooks::default().at(move |point| {
            assert_ne!(point, at_step(step), "crash");
        });
        let crashed = thread::scope(|scope| scope.spawn(|| convert(root, &hooks)).join());
        assert!(crashed.is_err(), "{step:?}: {crashed:?}");
    }

    #[test]
    fn a_crash_at_every_step_then_a_rerun_converts() {
        for step in STEPS {
            let root = tempfile::tempdir().unwrap();
            let standard = standard(None);
            plant(root.path(), &standard);
            crash_at(root.path(), step);
            let mirror = fs::read(root.path().join("repo")).unwrap();
            // Readers that don't implement the layout read the standard repo
            // info until the publication.
            assert_eq!(mirror == standard, step != FromStandardStep::Publish, "{step:?}");
            assert_eq!(backups(root.path()).len(), 1, "{step:?}");
            assert_eq!(
                tmp_dirs(root.path()).len(),
                usize::from(step == FromStandardStep::Build)
            );
            assert_eq!(
                root.path().join("repo.d").exists(),
                matches!(step, FromStandardStep::RenameDir | FromStandardStep::Publish),
                "{step:?}"
            );

            let expected = match step {
                FromStandardStep::Publish => ConversionOutcome::NothingToConvert,
                _ => ConversionOutcome::Converted,
            };
            assert_eq!(converted(root.path()), expected, "{step:?}");
            assert_converted(root.path(), &standard);
            // A rerun before the directory rename writes another backup.
            let reran_from_the_start =
                matches!(step, FromStandardStep::Backup | FromStandardStep::Build);
            assert_eq!(backups(root.path()).len(), 1 + usize::from(reran_from_the_start));
        }
    }

    #[test]
    fn leftover_tmp_dirs_of_earlier_runs_are_discarded() {
        let root = tempfile::tempdir().unwrap();
        let standard = standard(None);
        plant(root.path(), &standard);
        crash_at(root.path(), FromStandardStep::Build);
        crash_at(root.path(), FromStandardStep::Build);
        assert_eq!(tmp_dirs(root.path()).len(), 2);
        assert_eq!(converted(root.path()), ConversionOutcome::Converted);
        assert_converted(root.path(), &standard);
    }

    /// The state a crash after the directory rename leaves, then `$ROOT/repo`
    /// changed by a standard write.
    fn interrupted_then_forked(root: &Path) -> Vec<u8> {
        plant(root, &standard(None));
        crash_at(root, FromStandardStep::RenameDir);
        let forked = featureless_repo_info(7);
        plant(root, &forked);
        forked
    }

    #[test]
    fn a_rerun_stops_if_repo_changed_after_the_directory_rename() {
        let root = tempfile::tempdir().unwrap();
        let forked = interrupted_then_forked(root.path());
        let before = (names(root.path()), names(&root.path().join("repo.d")));
        let err = convert(root.path(), &NoHooks).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg)
                if msg.contains("publish_mirror()") && msg.contains("convert_to_filesystem_layout()")),
            "{err:?}"
        );
        assert_eq!((names(root.path()), names(&root.path().join("repo.d"))), before);
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), forked);
    }

    #[test]
    fn sabotage_from_standard_without_provenance_overwrites_a_standard_write() {
        let root = tempfile::tempdir().unwrap();
        interrupted_then_forked(root.path());
        let hooks =
            TestHooks::default().sabotage(Sabotage::FromStandardWithoutProvenance);
        assert_eq!(
            convert(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        // The planted change is lost.
        let Current { bytes, .. } = repo(root.path()).read_latest().unwrap().unwrap();
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), bytes);
    }

    /// A `repo.d` that step 1's listing missed appears before our directory
    /// rename: the rename fails with `EEXIST` or `ENOTEMPTY`, our tmp
    /// directory is discarded, and classifying again finds a finished
    /// conversion.
    #[test]
    fn a_repo_d_that_appears_before_the_rename_is_classified_again() {
        let root = tempfile::tempdir().unwrap();
        plant(root.path(), &standard(None));
        let refused = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&refused);
        let hooks = TestHooks::default().rename(move |from, to| {
            let renamed = fs::rename(from, to);
            if let Err(err) = &renamed
                && to.file_name().is_some_and(|n| n == "repo.d")
            {
                recorded.lock().unwrap().push(err.kind());
            }
            renamed
        });
        let mut theirs = None;
        let (outcome, reached) = with_paused(
            hooks,
            at_step(FromStandardStep::Build),
            |hooks| convert(root.path(), hooks).unwrap(),
            || {
                assert_eq!(tmp_dirs(root.path()).len(), 1);
                theirs = Some(repo_d_with(root.path(), &repo_info(5)));
                plant(root.path(), &repo_info(5));
            },
        );
        assert_eq!(outcome, Some(ConversionOutcome::NothingToConvert));
        let refused = refused.lock().unwrap().clone();
        assert!(
            matches!(
                refused.as_slice(),
                [io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty]
            ),
            "{refused:?}"
        );
        assert!(!reached.contains(&at_step(FromStandardStep::RenameDir)), "{reached:?}");
        assert_eq!(tmp_dirs(root.path()), Vec::<String>::new());
        let theirs = theirs.unwrap();
        assert_eq!(
            names(&root.path().join("repo.d")),
            set([theirs.file_name(), latest(&theirs)])
        );
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), repo_info(5));
    }

    /// An I/O failure in a step of the conversion names the conversion to
    /// rerun, and the cause; a rerun completes the conversion.
    #[test]
    fn io_failures_name_the_conversion_to_rerun() {
        let root = tempfile::tempdir().unwrap();
        let standard = standard(None);
        plant(root.path(), &standard);
        let hooks = TestHooks::default().rename(|from, to| {
            if to.file_name().is_some_and(|n| n == "repo.d") {
                Err(io::Error::new(io::ErrorKind::PermissionDenied, "injected"))
            } else {
                fs::rename(from, to)
            }
        });
        let err = convert(root.path(), &hooks).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg)
                if msg.contains("renaming the new repo.d into place failed: injected")
                    && msg.ends_with("rerun convert_to_filesystem_layout() to complete the conversion")),
            "{err:?}"
        );
        assert_eq!(tmp_dirs(root.path()), Vec::<String>::new());
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), standard);

        // The root becomes read-only once the backup is written.
        use std::os::unix::fs::PermissionsExt as _;
        let read_only = root.path().to_path_buf();
        let hooks = TestHooks::default().at(move |point| {
            if point == at_step(FromStandardStep::Backup) {
                fs::set_permissions(&read_only, fs::Permissions::from_mode(0o555))
                    .unwrap();
            }
        });
        let converted_anyway = convert(root.path(), &hooks);
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755)).unwrap();
        // Root writes anyway: nothing more to test.
        if let Err(err) = converted_anyway {
            assert!(
                matches!(&err, ProtocolError::Other(msg)
                    if msg.starts_with("building the new repo.d failed: ")
                        && msg.contains("rerun convert_to_filesystem_layout()")),
                "{err:?}"
            );
            assert_eq!(converted(root.path()), ConversionOutcome::Converted);
        }
        assert_converted(root.path(), &standard);
    }

    /// The directory rename landed and its reply was lost: the provenance
    /// check finds this run's backup.
    #[test]
    fn a_lost_reply_of_the_directory_rename_is_resolved_by_provenance() {
        let root = tempfile::tempdir().unwrap();
        let standard = standard(None);
        plant(root.path(), &standard);
        let hooks = lose_rename_reply_onto("repo.d");
        assert_eq!(
            convert(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert!(!hooks.reached().contains(&at_step(FromStandardStep::RenameDir)));
        assert_converted(root.path(), &standard);
        assert_eq!(backups(root.path()).len(), 1);
    }

    #[test]
    fn a_directory_that_keeps_vanishing_fails_after_classifying_again() {
        let root = tempfile::tempdir().unwrap();
        let standard = standard(None);
        plant(root.path(), &standard);
        let hooks = TestHooks::default().rename(|from, to| {
            if to.file_name().is_some_and(|n| n == "repo.d") {
                fs::remove_dir_all(from)?;
                Err(io::ErrorKind::NotFound.into())
            } else {
                fs::rename(from, to)
            }
        });
        let err = convert(root.path(), &hooks).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("rerun convert_to_filesystem_layout()")),
            "{err:?}"
        );
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), standard);
        assert!(!root.path().join("repo.d").exists());
        assert_eq!(backups(root.path()).len(), 2);
        assert_eq!(converted(root.path()), ConversionOutcome::Converted);
    }

    /// A change by a writer of the current version, with the backup core
    /// writes before it, paging the ops log at `num_updates`.
    fn writer_change(root: &Path, num_updates: u16) -> Version {
        let repo = repo(root);
        let Current { bytes, version } = repo.read_latest().unwrap().unwrap();
        let backup = repo_info_file::new_backup_path();
        fs::write(root.join("overwritten").join(&backup), &bytes).unwrap();
        let changed = metadata_changed_paged(&bytes, &backup, num_updates);
        match repo.conditional_update(&changed, &version).unwrap() {
            UpdateOutcome::Updated(version) => version,
            UpdateOutcome::NotOnLatestVersion => panic!("{version:?} is not current"),
        }
    }

    /// `bytes` after a change of the metadata whose predecessor entry names
    /// `backup`.
    fn metadata_changed(bytes: &[u8], backup: &str) -> Vec<u8> {
        metadata_changed_paged(bytes, backup, DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE)
    }

    fn metadata_changed_paged(bytes: &[u8], backup: &str, num_updates: u16) -> Vec<u8> {
        // Each update of the ops log is later than the one before.
        thread::sleep(Duration::from_millis(2));
        let info = repo_info_file::decode(bytes)
            .unwrap()
            .set_metadata(
                SpecVersionBin::current(),
                &Default::default(),
                backup,
                num_updates,
            )
            .unwrap();
        repo_info_file::encode(&info, SpecVersionBin::current(), 3).unwrap()
    }

    /// Writers change the repository between a crash after the directory
    /// rename and the rerun. The rerun finds the conversion's backup in an
    /// older ops-log entry and publishes, while it is named within three
    /// ops-log files; with one update per file, two changes push it past them.
    #[test]
    fn a_rerun_after_writers_finds_the_backup_within_three_ops_log_files() {
        for (num_updates, changes, found) in
            [(None, 3, true), (Some(1), 1, true), (Some(1), 2, false)]
        {
            let root = tempfile::tempdir().unwrap();
            let standard = standard(num_updates);
            plant(root.path(), &standard);
            crash_at(root.path(), FromStandardStep::RenameDir);
            let page = num_updates.unwrap_or(DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE);
            #[expect(
                clippy::double_ended_iterator_last,
                reason = "every change must run, not only the last"
            )]
            let last =
                (0..changes).map(|_| writer_change(root.path(), page)).last().unwrap();
            assert_eq!(fs::read(root.path().join("repo")).unwrap(), standard);

            let rerun = convert(root.path(), &NoHooks);
            let case = format!("{num_updates:?}, {changes}");
            if found {
                assert_eq!(rerun.unwrap(), Some(ConversionOutcome::Converted), "{case}");
                let current = root.path().join("repo.d").join(last.file_name());
                assert_eq!(
                    fs::read(root.path().join("repo")).unwrap(),
                    fs::read(current).unwrap(),
                    "{case}"
                );
            } else {
                let err = rerun.unwrap_err();
                assert!(
                    matches!(&err, ProtocolError::Other(msg) if msg.contains("needs manual recovery")),
                    "{case}: {err:?}"
                );
                assert_eq!(
                    fs::read(root.path().join("repo")).unwrap(),
                    standard,
                    "{case}"
                );
            }
        }
    }

    #[test]
    fn a_rerun_publishes_a_missing_mirror() {
        let root = tempfile::tempdir().unwrap();
        let standard = standard(None);
        plant(root.path(), &standard);
        converted(root.path());
        fs::remove_file(root.path().join("repo")).unwrap();
        assert_eq!(converted(root.path()), ConversionOutcome::NothingToConvert);
        assert_converted(root.path(), &standard);
    }

    #[test]
    fn a_rerun_with_an_unreadable_mirror_changes_nothing() {
        let root = tempfile::tempdir().unwrap();
        plant(root.path(), &standard(None));
        converted(root.path());
        plant(root.path(), UNDECODABLE);
        let before = names(&root.path().join("repo.d"));
        let err = convert(root.path(), &NoHooks).unwrap_err();
        assert!(matches!(err, ProtocolError::MirrorUnreadable(_)), "{err:?}");
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), UNDECODABLE);
        assert_eq!(names(&root.path().join("repo.d")), before);
    }

    /// The current version an interrupted conversion to the standard layout
    /// made.
    #[test]
    fn a_featureless_current_version_is_a_conversion_in_progress() {
        let root = tempfile::tempdir().unwrap();
        let v0 = create(root.path(), &NoHooks, &v0_bytes());
        assert!(matches!(
            repo(root.path()).conditional_update(&featureless_repo_info(1), &v0).unwrap(),
            UpdateOutcome::Updated(_)
        ));
        let before = (names(root.path()), names(&root.path().join("repo.d")));
        let err = convert(root.path(), &NoHooks).unwrap_err();
        assert!(matches!(err, ProtocolError::ConversionInProgress(_)), "{err:?}");
        assert_eq!((names(root.path()), names(&root.path().join("repo.d"))), before);
    }

    // ── convert_to_standard ─────────────────────────────────────────────────

    fn to_step(step: ToStandardStep) -> Point {
        Point::Conversion(ConversionStep::ToStandard(step))
    }

    fn to_standard(
        root: &Path,
        hooks: &dyn ProtocolHooks,
    ) -> Result<Option<ConversionOutcome>, ProtocolError> {
        RepoD { root, hooks }.convert_to_standard()
    }

    fn to_standard_converted(root: &Path) -> ConversionOutcome {
        to_standard(root, &NoHooks).unwrap().unwrap()
    }

    /// A repository in the filesystem layout, converted from a standard one
    /// whose configuration sets `num_updates`, with `changes` writer changes:
    /// its current version and bytes. The mirror is current.
    fn in_layout(root: &Path, num_updates: Option<u16>, changes: usize) -> Current {
        plant(root, &standard(num_updates));
        converted(root);
        let page = num_updates.unwrap_or(DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE);
        for _ in 0..changes {
            writer_change(root, page);
        }
        let current = repo(root).read_latest().unwrap().unwrap();
        plant(root, &current.bytes);
        current
    }

    /// Everything under the root, with the bytes of every file.
    fn tree(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        fn walk(base: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                let key = path.strip_prefix(base).unwrap().to_path_buf();
                if path.is_dir() {
                    out.insert(key, Vec::new());
                    walk(base, &path, out);
                } else {
                    out.insert(key, fs::read(&path).unwrap());
                }
            }
        }
        let mut out = BTreeMap::new();
        walk(root, root, &mut out);
        out
    }

    /// The end state of a conversion to the standard layout of the version
    /// with `before` bytes: no `repo.d` and no `repo.d.tmp.*`, `$ROOT/repo`
    /// that version without the feature and the gate, keeping its status,
    /// with one new `RepoStatusChangedUpdate` whose predecessor names a backup
    /// of `before`. Returns the bytes of `$ROOT/repo`.
    fn assert_standard(root: &Path, before: &[u8]) -> Vec<u8> {
        assert!(!root.join("repo.d").exists());
        assert_eq!(tmp_dirs(root), Vec::<String>::new());
        let bytes = fs::read(root.join("repo")).unwrap();
        assert!(!layout_v1::lists_feature(&bytes).unwrap());
        let (before_info, after) = (
            repo_info_file::decode(before).unwrap(),
            repo_info_file::decode(&bytes).unwrap(),
        );
        let stored = serde_json::to_value(after.raw_status().unwrap()).unwrap();
        assert!(stored.get("underlying").is_none(), "{stored}");
        assert_eq!(after.status().unwrap(), before_info.status().unwrap());
        let entries: Vec<_> =
            after.latest_updates().unwrap().map(Result::unwrap).collect();
        let older: Vec<_> =
            before_info.latest_updates().unwrap().map(Result::unwrap).collect();
        assert!(
            matches!(entries[0].0, UpdateType::RepoStatusChangedUpdate { .. }),
            "{entries:?}"
        );
        if after.repo_before_updates().unwrap().is_none() {
            let kinds = |entries: &[(UpdateType, DateTime<Utc>, Option<&str>)]| {
                entries
                    .iter()
                    .map(|(kind, at, _)| (kind.clone(), *at))
                    .collect::<Vec<_>>()
            };
            assert_eq!(kinds(&entries[1..]), kinds(&older));
        }
        let attempt = layout_v1::attempt_id(&bytes).unwrap().unwrap();
        assert_eq!(backup(root, &attempt), before);
        bytes
    }

    #[test]
    fn to_standard_converts_the_current_version() {
        let root = tempfile::tempdir().unwrap();
        let Current { bytes, version } = in_layout(root.path(), None, 2);
        let backups_before = backups(root.path()).len();
        let hooks = TestHooks::default();
        assert_eq!(
            to_standard(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert_standard(root.path(), &bytes);
        let attempt =
            layout_v1::attempt_id(&fs::read(root.path().join("repo")).unwrap()).unwrap();
        assert_eq!(Hash::of(&backup(root.path(), &attempt.unwrap())), *version.hash());
        assert_eq!(backups(root.path()).len(), backups_before + 1);
        assert_eq!(
            hooks.reached(),
            [
                to_step(ToStandardStep::ForkCheck),
                Point::BeforeMirrorRead,
                Point::Staged,
                Point::Linked,
                Point::BeforeRename,
                Point::Renamed,
                Point::BeforeTidyUp,
                to_step(ToStandardStep::Change),
                Point::BeforePublish,
                Point::MirrorStaged,
                Point::BeforeMirrorRead,
                Point::BeforeMirrorRename,
                Point::MirrorRenamed,
                to_step(ToStandardStep::Publish),
                to_step(ToStandardStep::RenameDir),
                to_step(ToStandardStep::DeleteDir),
            ]
        );
        assert_eq!(repo(root.path()).open_status().unwrap(), OpenStatus::StandardLayout);

        // Done: nothing changes.
        let before = tree(root.path());
        assert_eq!(
            to_standard_converted(root.path()),
            ConversionOutcome::NothingToConvert
        );
        assert_eq!(tree(root.path()), before);

        // And back.
        assert_eq!(converted(root.path()), ConversionOutcome::Converted);
    }

    #[test]
    fn to_standard_pages_at_the_repositorys_own_size() {
        for (num_updates, paged) in [(None, false), (Some(1), true)] {
            let root = tempfile::tempdir().unwrap();
            let Current { bytes, .. } = in_layout(root.path(), num_updates, 1);
            assert_eq!(to_standard_converted(root.path()), ConversionOutcome::Converted);
            let standard = assert_standard(root.path(), &bytes);
            let named = layout_v1::backup_paths(&standard).unwrap();
            assert_eq!(named.repo_before_updates.is_some(), paged, "{num_updates:?}");
            assert_eq!(named.entries.is_empty(), paged, "{num_updates:?}");
        }
    }

    #[test]
    fn to_standard_keeps_the_repositorys_own_status() {
        let root = tempfile::tempdir().unwrap();
        let read_only = RepoStatus {
            availability: RepoAvailability::ReadOnly,
            set_at: DateTime::from_timestamp(1_700_000_001, 0).unwrap(),
            limited_availability_reason: Some("copying".to_string()),
        };
        let info = repo_info_file::decode(&standard(None))
            .unwrap()
            .set_status(SpecVersionBin::current(), &read_only, Some(("repo.s", 10)))
            .unwrap();
        plant(
            root.path(),
            &repo_info_file::encode(&info, SpecVersionBin::current(), 3).unwrap(),
        );
        converted(root.path());
        let Current { bytes, .. } = repo(root.path()).read_latest().unwrap().unwrap();
        assert_eq!(to_standard_converted(root.path()), ConversionOutcome::Converted);
        let standard = assert_standard(root.path(), &bytes);
        assert_eq!(
            repo_info_file::decode(&standard).unwrap().status().unwrap(),
            read_only
        );
    }

    #[test]
    fn to_standard_without_a_repository_in_the_layout() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(to_standard(root.path(), &NoHooks).unwrap(), None);
        plant(root.path(), &repo_info(1));
        assert_eq!(to_standard(root.path(), &NoHooks).unwrap(), None);
        assert_eq!(names(root.path()), strings(["repo"]));

        // A standard repository: only leftover tmp directories are deleted.
        plant(root.path(), &standard(None));
        fs::create_dir_all(root.path().join(tmp_dir_name(Uuid::new_v4())).join("x"))
            .unwrap();
        assert_eq!(
            to_standard_converted(root.path()),
            ConversionOutcome::NothingToConvert
        );
        assert_eq!(names(root.path()), strings(["repo"]));
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), standard(None));

        let refused = |planted: &[u8]| {
            let root = tempfile::tempdir().unwrap();
            plant(root.path(), planted);
            let err = to_standard(root.path(), &NoHooks).unwrap_err();
            assert_eq!(names(root.path()), strings(["repo"]));
            assert_eq!(fs::read(root.path().join("repo")).unwrap(), planted);
            err
        };
        let err = refused(&changed_repo_info(1));
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("an orphan mirror")),
            "{err:?}"
        );
        let err = refused(UNDECODABLE);
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("can't be read or decoded and")),
            "{err:?}"
        );
    }

    #[test]
    fn to_standard_refuses_a_fork_or_an_unreadable_mirror_changing_nothing() {
        for planted in [featureless_repo_info(9), Vec::new()] {
            let root = tempfile::tempdir().unwrap();
            in_layout(root.path(), None, 1);
            plant(root.path(), &planted);
            let before = tree(root.path());
            let err = to_standard(root.path(), &NoHooks).unwrap_err();
            if planted.is_empty() {
                assert!(matches!(err, ProtocolError::MirrorUnreadable(_)), "{err:?}");
            } else {
                assert!(matches!(err, ProtocolError::Forked(_)), "{err:?}");
            }
            assert_eq!(tree(root.path()), before);
        }
    }

    /// The backup is the current version read from `repo.d`, never the
    /// lagging mirror.
    #[test]
    fn to_standard_backs_up_the_current_version_not_a_lagging_mirror() {
        let root = tempfile::tempdir().unwrap();
        let Current { bytes: lagging, .. } = in_layout(root.path(), None, 0);
        writer_change(root.path(), DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE);
        let Current { bytes, .. } = repo(root.path()).read_latest().unwrap().unwrap();
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), lagging);
        assert_eq!(to_standard_converted(root.path()), ConversionOutcome::Converted);
        assert_standard(root.path(), &bytes);
    }

    /// The listing the conversion read lagged behind a change: its own change
    /// conflicts, and it lists again and converts the current version.
    #[test]
    fn a_stale_listing_makes_the_change_conflict_then_converts_the_current_version() {
        let root = tempfile::tempdir().unwrap();
        in_layout(root.path(), None, 0);
        let backups_before = backups(root.path()).len();
        let writer_root = root.path().to_path_buf();
        let changed = AtomicBool::new(false);
        let hooks = TestHooks::default().at(move |point| {
            if point == to_step(ToStandardStep::ForkCheck)
                && !changed.swap(true, Ordering::SeqCst)
            {
                writer_change(&writer_root, DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE);
            }
        });
        assert_eq!(
            to_standard(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert_eq!(fork_checks(&hooks), 2, "{:?}", hooks.reached());
        // The writer's change is in the converted version.
        let standard = fs::read(root.path().join("repo")).unwrap();
        let entries: Vec<_> = repo_info_file::decode(&standard)
            .unwrap()
            .latest_updates()
            .unwrap()
            .map(|entry| entry.unwrap().0)
            .collect();
        assert!(matches!(entries[1], UpdateType::MetadataChangedUpdate), "{entries:?}");
        // The writer's and the change's: the conflict proved that the first
        // attempt didn't land, which deleted its backup.
        assert_eq!(backups(root.path()).len(), backups_before + 2);
    }

    /// The marker rename landed and its reply was lost, and a later change
    /// moved the marker on: the lost-reply check finds the attempt in the ops
    /// log of the current version.
    #[test]
    fn a_lost_reply_of_the_change_is_resolved_through_the_ops_log() {
        let root = tempfile::tempdir().unwrap();
        in_layout(root.path(), None, 0);
        let dir = root.path().join("repo.d");
        let later = Arc::new(Mutex::new(None));
        let recorded = Arc::clone(&later);
        let lost = AtomicBool::new(false);
        let hooks = TestHooks::default().rename(move |from, to| {
            fs::rename(from, to)?;
            let name = to.file_name().unwrap().to_str().unwrap();
            if let Some(RepoDName::Marker(marker)) = parse_repo_d_name(name)
                && !lost.swap(true, Ordering::SeqCst)
            {
                let ours = fs::read(dir.join(marker.version.file_name())).unwrap();
                let bytes = metadata_changed(&ours, "later");
                let next = marker.version.next(Hash::of(&bytes)).unwrap();
                fs::write(dir.join(next.file_name()), &bytes).unwrap();
                fs::rename(to, dir.join(latest(&next))).unwrap();
                *recorded.lock().unwrap() = Some(bytes);
                return Err(io::ErrorKind::NotFound.into());
            }
            Ok(())
        });
        assert_eq!(
            to_standard(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert!(
            hooks.observed().contains(&Observed::RenameSuperseded),
            "{:?}",
            hooks.observed()
        );
        assert!(hooks.reached().contains(&to_step(ToStandardStep::Change)));
        // The publication moved on to the later version.
        let later = later.lock().unwrap().clone().unwrap();
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), later);
        assert!(!root.path().join("repo.d").exists());
        assert_eq!(tmp_dirs(root.path()), Vec::<String>::new());
    }

    #[test]
    fn a_lost_reply_of_the_rename_aside_is_ours() {
        let root = tempfile::tempdir().unwrap();
        let Current { bytes, .. } = in_layout(root.path(), None, 1);
        let lost = AtomicBool::new(false);
        let hooks = TestHooks::default().rename(move |from, to| {
            fs::rename(from, to)?;
            if from.file_name().is_some_and(|n| n == "repo.d")
                && !lost.swap(true, Ordering::SeqCst)
            {
                Err(io::ErrorKind::NotFound.into())
            } else {
                Ok(())
            }
        });
        assert_eq!(
            to_standard(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert!(hooks.reached().contains(&to_step(ToStandardStep::RenameDir)));
        assert_standard(root.path(), &bytes);
    }

    const TO_STEPS: [ToStandardStep; 5] = [
        ToStandardStep::ForkCheck,
        ToStandardStep::Change,
        ToStandardStep::Publish,
        ToStandardStep::RenameDir,
        ToStandardStep::DeleteDir,
    ];

    /// Runs a conversion to the standard layout that crashes at `step`.
    fn crash_to_standard_at(root: &Path, step: ToStandardStep, hooks: TestHooks) {
        let hooks = hooks.at(move |point| {
            assert_ne!(point, to_step(step), "crash");
        });
        let crashed =
            thread::scope(|scope| scope.spawn(|| to_standard(root, &hooks)).join());
        assert!(crashed.is_err(), "{step:?}: {crashed:?}");
    }

    /// The same end state whatever step a crash interrupts, and between the
    /// change and the end of the rerun, implementations of the layout refuse
    /// to publish and to collect garbage.
    #[test]
    fn a_crash_at_every_step_then_a_rerun_converts_to_standard() {
        for step in TO_STEPS {
            let root = tempfile::tempdir().unwrap();
            let Current { bytes, .. } = in_layout(root.path(), None, 1);
            let backups_before = backups(root.path()).len();
            crash_to_standard_at(root.path(), step, TestHooks::default());
            let repo = repo(root.path());
            let changed = step != ToStandardStep::ForkCheck;
            let published =
                !matches!(step, ToStandardStep::ForkCheck | ToStandardStep::Change);
            match repo.read_latest().unwrap() {
                Some(Current { bytes: current, .. }) if changed => {
                    assert!(!layout_v1::lists_feature(&current).unwrap(), "{step:?}");
                    let err = repo.publish_mirror().unwrap_err();
                    assert!(
                        matches!(err, ProtocolError::ConversionInProgress(_)),
                        "{step:?}: {err:?}"
                    );
                    let err = repo.refuse_gc_without_change().unwrap_err();
                    assert!(
                        matches!(err, ProtocolError::ConversionInProgress(_)),
                        "{step:?}: {err:?}"
                    );
                }
                Some(Current { bytes: current, .. }) => assert_eq!(current, bytes),
                None => assert!(
                    matches!(step, ToStandardStep::RenameDir | ToStandardStep::DeleteDir),
                    "{step:?}"
                ),
            }
            let mirror = fs::read(root.path().join("repo")).unwrap();
            assert_eq!(mirror == bytes, !published, "{step:?}");
            assert_eq!(
                tmp_dirs(root.path()).len(),
                usize::from(step == ToStandardStep::RenameDir),
                "{step:?}"
            );

            let expected = if matches!(
                step,
                ToStandardStep::RenameDir | ToStandardStep::DeleteDir
            ) {
                ConversionOutcome::NothingToConvert
            } else {
                ConversionOutcome::Converted
            };
            assert_eq!(to_standard_converted(root.path()), expected, "{step:?}");
            let standard = assert_standard(root.path(), &bytes);
            if published {
                assert_eq!(standard, mirror, "{step:?}");
            }
            // One change in all: the crashed run's, or the rerun's.
            assert_eq!(backups(root.path()).len(), backups_before + 1, "{step:?}");
        }
    }

    /// A fork or an unreadable `$ROOT/repo` found by the publication after the
    /// change landed: nothing is published, `repo.d` is kept, and the error
    /// names the conversion to rerun; the rerun finishes once `$ROOT/repo` is
    /// moved aside.
    #[test]
    fn a_fork_or_an_unreadable_mirror_after_the_change_names_the_rerun() {
        for planted in [featureless_repo_info(9), Vec::new()] {
            let root = tempfile::tempdir().unwrap();
            let Current { bytes, .. } = in_layout(root.path(), None, 1);
            let mirror = root.path().join("repo");
            let (at_change, to_plant) = (mirror.clone(), planted.clone());
            let hooks = TestHooks::default().at(move |point| {
                if point == to_step(ToStandardStep::Change) {
                    fs::write(&at_change, &to_plant).unwrap();
                }
            });
            let err = to_standard(root.path(), &hooks).unwrap_err();
            let detail = match (&err, planted.is_empty()) {
                (ProtocolError::MirrorUnreadable(detail), true) => {
                    assert!(detail.contains("readable or move it aside"), "{detail}");
                    detail
                }
                (ProtocolError::Forked(detail), false) => {
                    assert!(detail.contains("section on forks"), "{detail}");
                    assert!(
                        detail.contains("not convert_to_filesystem_layout()"),
                        "{detail}"
                    );
                    detail
                }
                _ => panic!("{err:?}"),
            };
            assert!(detail.contains(TO_STANDARD_RERUN), "{detail}");
            assert!(!hooks.reached().contains(&to_step(ToStandardStep::Publish)));
            assert_eq!(fs::read(&mirror).unwrap(), planted);
            assert!(root.path().join("repo.d").exists());

            fs::remove_file(&mirror).unwrap();
            assert_eq!(to_standard_converted(root.path()), ConversionOutcome::Converted);
            assert_standard(root.path(), &bytes);
        }
    }

    /// What a crash after the publication leaves: the current version without
    /// the feature and its copy in `$ROOT/repo`. Returns the version before
    /// the change, its bytes, and the version without the feature.
    fn published_then_crashed(root: &Path) -> (Version, Vec<u8>, Version) {
        let Current { bytes, version } = in_layout(root, None, 1);
        crash_to_standard_at(root, ToStandardStep::Publish, TestHooks::default());
        let featureless = repo(root).read_latest().unwrap().unwrap().version;
        (version, bytes.to_vec(), featureless)
    }

    /// A listing of `repo.d` that still shows `before` as current, after the
    /// change to `after`.
    fn stale_view(root: &Path, before: &Version, bytes: &[u8], after: &Version) {
        let dir = root.join("repo.d");
        fs::write(dir.join(before.file_name()), bytes).unwrap();
        fs::rename(dir.join(latest(after)), dir.join(latest(before))).unwrap();
    }

    fn fresh_view(root: &Path, before: &Version, after: &Version) {
        let dir = root.join("repo.d");
        fs::rename(dir.join(latest(before)), dir.join(latest(after))).unwrap();
        fs::remove_file(dir.join(before.file_name())).unwrap();
    }

    fn fork_checks(hooks: &TestHooks) -> usize {
        hooks
            .reached()
            .iter()
            .filter(|point| **point == to_step(ToStandardStep::ForkCheck))
            .count()
    }

    /// A rerun whose listing shows the version before the earlier run's change,
    /// while `$ROOT/repo` holds the published version without the feature:
    /// the fork check finds it a copy of a newer version, and the rerun lists
    /// again instead of reporting a fork.
    #[test]
    fn a_rerun_under_a_stale_listing_lists_again_and_finishes() {
        let root = tempfile::tempdir().unwrap();
        let (before, bytes, after) = published_then_crashed(root.path());
        let published = fs::read(root.path().join("repo")).unwrap();
        stale_view(root.path(), &before, &bytes, &after);
        let backups_before = backups(root.path()).len();
        let refresh_root = root.path().to_path_buf();
        let (b, a) = (before.clone(), after.clone());
        let refreshed = AtomicBool::new(false);
        let hooks = TestHooks::default().at(move |point| {
            if point == to_step(ToStandardStep::ForkCheck)
                && !refreshed.swap(true, Ordering::SeqCst)
            {
                fresh_view(&refresh_root, &b, &a);
            }
        });
        assert_eq!(
            to_standard(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert_eq!(fork_checks(&hooks), 1);
        assert!(!hooks.reached().contains(&to_step(ToStandardStep::Change)));
        assert_eq!(assert_standard(root.path(), &bytes), published);
        assert_eq!(backups(root.path()).len(), backups_before);
    }

    /// Every relisting stays stale: a retryable error, not a fork, and
    /// nothing changed; a run with fresh listings finishes.
    #[test]
    fn a_listing_stale_through_every_relisting_fails_retryably() {
        let root = tempfile::tempdir().unwrap();
        let (before, bytes, after) = published_then_crashed(root.path());
        stale_view(root.path(), &before, &bytes, &after);
        let unchanged = tree(root.path());
        let hooks = TestHooks::default();
        let err = to_standard(root.path(), &hooks).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg)
                if msg.contains("run convert_to_standard_layout() again")),
            "{err:?}"
        );
        assert_eq!(fork_checks(&hooks), RELISTS + 1);
        assert_eq!(tree(root.path()), unchanged);

        fresh_view(root.path(), &before, &after);
        assert_eq!(to_standard_converted(root.path()), ConversionOutcome::Converted);
        assert_standard(root.path(), &bytes);
    }

    /// The fork check's own listing misses the version file too: a false
    /// fork report, nothing changed; a run with fresh listings finishes.
    #[test]
    fn a_stale_fork_check_listing_reports_a_false_fork_and_changes_nothing() {
        let root = tempfile::tempdir().unwrap();
        let (before, bytes, after) = published_then_crashed(root.path());
        stale_view(root.path(), &before, &bytes, &after);
        let (file, hidden) = (
            root.path().join("repo.d").join(after.file_name()),
            root.path().join("hidden"),
        );
        fs::rename(&file, &hidden).unwrap();
        let unchanged = tree(root.path());
        let err = to_standard(root.path(), &NoHooks).unwrap_err();
        assert!(matches!(err, ProtocolError::Forked(_)), "{err:?}");
        assert_eq!(tree(root.path()), unchanged);

        fs::rename(&hidden, &file).unwrap();
        fresh_view(root.path(), &before, &after);
        assert_eq!(to_standard_converted(root.path()), ConversionOutcome::Converted);
        assert_standard(root.path(), &bytes);
    }

    /// `publish_mirror()` between the interrupted run and its rerun, from a
    /// listing that shows the version before the change, adopts the newer
    /// version in place.
    #[test]
    fn publish_mirror_under_a_stale_listing_keeps_the_published_version() {
        let root = tempfile::tempdir().unwrap();
        let (before, bytes, after) = published_then_crashed(root.path());
        let published = fs::read(root.path().join("repo")).unwrap();
        stale_view(root.path(), &before, &bytes, &after);
        assert_eq!(
            repo(root.path()).publish_mirror().unwrap(),
            Some(PublishOutcome::Current)
        );
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), published);

        fresh_view(root.path(), &before, &after);
        assert_eq!(to_standard_converted(root.path()), ConversionOutcome::Converted);
        assert_eq!(assert_standard(root.path(), &bytes), published);
    }

    /// A conversion from the standard layout interrupted after its directory
    /// rename: the standard `$ROOT/repo` is no version of `repo.d`, so the
    /// conversion to the standard layout stops before changing anything.
    #[test]
    fn to_standard_during_an_interrupted_conversion_from_standard_is_a_fork() {
        let root = tempfile::tempdir().unwrap();
        plant(root.path(), &standard(None));
        crash_at(root.path(), FromStandardStep::RenameDir);
        let unchanged = tree(root.path());
        let err = to_standard(root.path(), &NoHooks).unwrap_err();
        assert!(matches!(err, ProtocolError::Forked(_)), "{err:?}");
        assert_eq!(tree(root.path()), unchanged);

        assert_eq!(converted(root.path()), ConversionOutcome::Converted);
        let Current { bytes, .. } = repo(root.path()).read_latest().unwrap().unwrap();
        assert_eq!(to_standard_converted(root.path()), ConversionOutcome::Converted);
        assert_standard(root.path(), &bytes);
    }

    /// A change lands after the version the conversion read: without its own
    /// change, the conversion publishes the stale version, and the
    /// acknowledged change is lost.
    #[test]
    fn sabotage_to_standard_without_change_loses_an_acknowledged_change() {
        let root = tempfile::tempdir().unwrap();
        let Current { bytes, .. } = in_layout(root.path(), None, 0);
        let writer_root = root.path().to_path_buf();
        let changed = AtomicBool::new(false);
        let hooks = TestHooks::default().sabotage(Sabotage::ToStandardWithoutChange).at(
            move |point| {
                if point == to_step(ToStandardStep::ForkCheck)
                    && !changed.swap(true, Ordering::SeqCst)
                {
                    writer_change(&writer_root, DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE);
                }
            },
        );
        assert_eq!(
            to_standard(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert!(!hooks.reached().contains(&Point::Staged));
        // The stale version, without the writer's change.
        let standard = assert_standard(root.path(), &bytes);
        let entries: Vec<_> = repo_info_file::decode(&standard)
            .unwrap()
            .latest_updates()
            .unwrap()
            .map(|entry| entry.unwrap().0)
            .collect();
        assert!(
            !entries
                .iter()
                .any(|entry| matches!(entry, UpdateType::MetadataChangedUpdate)),
            "{entries:?}"
        );
    }

    /// `repo.d` deleted before the publication, then a crash: the older
    /// mirror with the feature and no `repo.d`, an orphan mirror; the
    /// conversion's change is lost.
    #[test]
    fn sabotage_delete_dir_before_publish_leaves_an_orphan_mirror() {
        let root = tempfile::tempdir().unwrap();
        let Current { bytes, .. } = in_layout(root.path(), None, 1);
        let hooks = TestHooks::default().sabotage(Sabotage::DeleteDirBeforePublish);
        crash_to_standard_at(root.path(), ToStandardStep::DeleteDir, hooks);
        assert!(!root.path().join("repo.d").exists());
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), bytes);
        assert_eq!(repo(root.path()).open_status().unwrap(), OpenStatus::OrphanMirror);
        let err = to_standard(root.path(), &NoHooks).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("an orphan mirror")),
            "{err:?}"
        );
    }
}
