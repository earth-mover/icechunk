//! The administrative conversions between the object store layout and the
//! filesystem layout v1, see "Conversion from the object store layout" and
//! "Conversion to the object store layout" in design-docs/020-filesystem-storage.md.

use std::{fs, io};

use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use icechunk_format::{
    IcechunkFormatError, IcechunkResult, OVERWRITTEN_FILES_PATH,
    layout_v1::{self, FeatureChange},
    repo_info::{DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE, RepoInfo},
    repo_info_file,
    writer_features::{min_version, missing_features},
};
use uuid::Uuid;

use super::{
    OPS_LOG_FILES, ProtocolError, RepoD,
    commit::{ChangeBytes, UpdateOutcome},
    discard_tmp_dir, is_stale,
    lifecycle::{FEATURES, RootRepo, unix_millis, write_marker},
    read::Current,
    remove_stage, with_retries, with_root_lock,
};
use crate::{
    hooks::{ConversionStep, FromObjectStoreStep, Point, Sabotage, ToObjectStoreStep},
    names::{Hash, REPO, RootName, Version, parse_root_name, tmp_dir_name},
    nudge::nudge,
    path::{key_path, staging_path},
    read::read_all,
    storage::is_missing,
    write::{CreateOutcome as Written, create_exclusive, fsync_dir, write_fresh},
};

const RERUN: &str = "rerun convert_to_filesystem_layout() to complete the conversion";
const TO_OBJECT_STORE_RERUN: &str =
    "rerun convert_to_object_store_layout() to complete the conversion";
const OBJECT_STORE_INFO: &str = "the repo info in the object store layout";
/// The retries of the change of the conversion to the object store layout
/// after a conflict, which shows that its listing of `repo.d` was stale.
const RELISTS: usize = 8;

/// What [`FilesystemStorage::convert_to_filesystem_layout`] or
/// [`FilesystemStorage::convert_to_object_store_layout`] did.
///
/// [`FilesystemStorage::convert_to_filesystem_layout`]: crate::FilesystemStorage::convert_to_filesystem_layout
/// [`FilesystemStorage::convert_to_object_store_layout`]: crate::FilesystemStorage::convert_to_object_store_layout
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversionOutcome {
    /// The repository was converted, by this call or by an interrupted run
    /// that this call completed.
    Converted,
    /// The repository was in the target layout already. For the conversion to
    /// the object store layout, that includes an interrupted run that had renamed
    /// `repo.d` aside: this call only deleted what it left.
    NothingToConvert,
}

impl RepoD<'_> {
    /// `FilesystemStorage::convert_to_filesystem_layout`. Administrative:
    /// nothing else may use the repository. A rerun after a crash decides from
    /// the state it finds. `None` if there is no repository.
    pub(crate) fn convert_from_object_store(
        &self,
    ) -> Result<Option<ConversionOutcome>, ProtocolError> {
        let mut classified_again = false;
        let outcome = loop {
            nudge(self.root);
            if self.repo_d_exists()? {
                break self.resume_conversion()?;
            }
            let object_store_info = match self.root_repo()? {
                RootRepo::ObjectStore(bytes) => bytes,
                RootRepo::Missing => return Ok(None),
                RootRepo::Undecodable(why) => {
                    return Err(ProtocolError::repo_unreadable(self.root, &why));
                }
            };
            match self.convert_object_store(&object_store_info)? {
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

    /// Steps 2 to 7 on the repo info `object_store_info`. `None` if the
    /// directory rename found a `repo.d` or its directory gone: classify again.
    fn convert_object_store(
        &self,
        object_store_info: &[u8],
    ) -> Result<Option<ConversionOutcome>, ProtocolError> {
        let (num_updates, now) = check_convertible(object_store_info, OBJECT_STORE_INFO)?;
        let backup = repo_info_file::new_backup_path();
        let bytes = layout_v1::convert(
            object_store_info,
            FeatureChange::Add,
            &backup,
            now,
            num_updates,
        )
        .map_err(|err| unconvertible(OBJECT_STORE_INFO, &err))?;
        self.write_backup(&backup, object_store_info).map_err(|err| {
            interrupted(
                "writing the backup of the repo info in the object store layout",
                err,
            )
        })?;
        self.hooks.at(from_object_store(FromObjectStoreStep::Backup));

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
        self.hooks.at(from_object_store(FromObjectStoreStep::Build));

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
        self.hooks.at(from_object_store(FromObjectStoreStep::RenameDir));
        self.unlink_object_store_repo()?;
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
        nudge(self.root);
        let repo = self.root.join(REPO);
        let on_disk = match with_root_lock(self.root, || read_all(&repo)) {
            Ok(on_disk) => on_disk,
            Err(err) if is_missing(&err) => {
                return Ok(ConversionOutcome::NothingToConvert);
            }
            Err(err) => {
                return Err(self.foreign_root_repo(&format!("can't be read ({err})")));
            }
        };
        if !self.hooks.sabotaged(Sabotage::FromObjectStoreWithoutProvenance) {
            let foreign = match layout_v1::lists_feature(&on_disk) {
                Ok(false) if self.built_from(&bytes, &on_disk)? => None,
                Ok(false) => Some(format!(
                    "holds a repo info this conversion didn't build {} from: no backup \
                     named or linked in the last {OPS_LOG_FILES} ops-log files of {} holds \
                     its bytes, so it changed after the conversion renamed {} into place, \
                     or more changes landed since the conversion than those files hold",
                    dir.display(),
                    describe(),
                    dir.display(),
                )),
                Ok(true) => Some(format!(
                    "holds a repo info that lists {}, which this conversion didn't build \
                     {} from",
                    layout_v1::FEATURE,
                    dir.display()
                )),
                Err(err) => Some(format!("doesn't decode ({err})")),
            };
            if let Some(what) = foreign {
                return Err(self.foreign_root_repo(&what));
            }
        }
        self.unlink_object_store_repo()?;
        Ok(ConversionOutcome::Converted)
    }

    /// `$ROOT/repo` next to `repo.d` that the provenance check can't attribute
    /// to this conversion: `what` it is.
    fn foreign_root_repo(&self, what: &str) -> ProtocolError {
        let repo = self.root.join(REPO);
        let dir = self.dir();
        ProtocolError::manual_recovery(
            self.root,
            &format!(
                "{repo} next to {dir} {what}. Nothing was changed. To keep {dir}, delete \
                 {repo} after checking that it holds nothing to keep; to keep {repo}, \
                 archive and delete {dir}, then run convert_to_filesystem_layout()",
                repo = repo.display(),
                dir = dir.display(),
            ),
        )
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
    /// ops log of `current` holds `object_store_info`. Reads at most
    /// [`OPS_LOG_FILES`] files of the ops log, newest first: `current`, then
    /// the backups that each file's `repo_before_updates` names.
    fn built_from(
        &self,
        current: &[u8],
        object_store_info: &[u8],
    ) -> Result<bool, ProtocolError> {
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
                    .read_backup(backup, object_store_info.len())?
                    .is_some_and(|b| b == object_store_info)
                {
                    return Ok(true);
                }
            }
            let Some(before) = named.repo_before_updates.filter(|_| read < OPS_LOG_FILES)
            else {
                return Ok(false);
            };
            match self.read_backup(&before, None)? {
                Some(bytes) if bytes == object_store_info => return Ok(true),
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

    /// Step 7: unlinks `$ROOT/repo`, gone already after a lost reply, then
    /// fsyncs the root.
    fn unlink_object_store_repo(&self) -> Result<(), ProtocolError> {
        let repo = self.root.join(REPO);
        let unlinking = |err: io::Error| {
            interrupted(&format!("unlinking {}", repo.display()), err.into())
        };
        match with_root_lock(self.root, || self.hooks.unlink(&repo)) {
            Err(err) if !is_missing(&err) => return Err(unlinking(err)),
            _ => {}
        }
        fsync_dir(self.root).map_err(unlinking)?;
        self.hooks.at(from_object_store(FromObjectStoreStep::RepoUnlinked));
        Ok(())
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

/// The current version without the feature, which the conversion to the
/// object store layout writes to `$ROOT/repo`.
struct Featureless {
    bytes: Bytes,
    /// Whether this run made the change; `false` on a rerun that found it
    /// made.
    changed: bool,
}

impl RepoD<'_> {
    /// `FilesystemStorage::convert_to_object_store_layout`. Administrative:
    /// nothing else may use the repository. A rerun after a crash decides from
    /// the state it finds. `None` if there is no repository.
    pub(crate) fn convert_to_object_store(
        &self,
    ) -> Result<Option<ConversionOutcome>, ProtocolError> {
        let mut classified_again = false;
        let outcome = loop {
            nudge(self.root);
            if !self.repo_d_exists()? {
                match self.root_repo()? {
                    // An earlier run renamed `repo.d` aside, or the repository
                    // is in the object store layout already.
                    RootRepo::ObjectStore(_) => {
                        break ConversionOutcome::NothingToConvert;
                    }
                    RootRepo::Missing => return Ok(None),
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
                        "{} was listed and then missing twice; {TO_OBJECT_STORE_RERUN}",
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
        let Some(featureless) = self.change_to_object_store()? else {
            return Ok(None);
        };
        if self.hooks.sabotaged(Sabotage::DeleteDirBeforeRepoWrite)
            && self.rename_aside()?
        {
            self.hooks.at(to_object_store(ToObjectStoreStep::RenameDir));
            self.delete_tmp_dirs()?;
        }
        self.create_object_store_repo(&featureless)?;
        self.hooks.at(to_object_store(ToObjectStoreStep::RepoWritten));
        if !self.rename_aside()? {
            return Ok(None);
        }
        self.hooks.at(to_object_store(ToObjectStoreStep::RenameDir));
        Ok(Some(()))
    }

    /// Steps 2 to 4: the current version without the feature, made by this
    /// run's change or found made by an earlier run's. `None` if `repo.d`
    /// doesn't exist.
    fn change_to_object_store(&self) -> Result<Option<Featureless>, ProtocolError> {
        let dir = self.dir();
        let mut conflicts = 0;
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
                Ok(false) => return Ok(Some(Featureless { bytes, changed: false })),
                Ok(true) => {}
                Err(err) => {
                    return Err(ProtocolError::Corrupt(format!(
                        "{current} doesn't decode: {err}"
                    )));
                }
            }
            let (num_updates, now) = check_convertible(&bytes, &current)?;

            self.hooks.at(to_object_store(ToObjectStoreStep::BeforeChange));
            self.refuse_a_root_repo()?;
            let backup = repo_info_file::new_backup_path();
            self.write_backup(&backup, &bytes).map_err(|err| {
                to_object_store_interrupted(
                    "writing the backup of the current version",
                    err,
                )
            })?;
            let converted = layout_v1::convert(
                &bytes,
                FeatureChange::Remove,
                &backup,
                now,
                num_updates,
            )
            .and_then(|bytes| ChangeBytes::unchecked(Bytes::from(bytes)))
            .map_err(|err| unconvertible(&current, &err))?;
            let outcome = if self.hooks.sabotaged(Sabotage::ToObjectStoreWithoutChange) {
                UpdateOutcome::Updated(version.next(Hash::of(&converted.bytes))?)
            } else {
                self.change(&converted, &version).map_err(|err| {
                    to_object_store_interrupted(
                        "the change to the version without the feature",
                        err,
                    )
                })?
            };
            match outcome {
                UpdateOutcome::Updated(_) => {
                    self.hooks.at(to_object_store(ToObjectStoreStep::Change));
                    return Ok(Some(Featureless {
                        bytes: converted.bytes,
                        changed: true,
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
                         convert_to_object_store_layout() again",
                        dir.display()
                    )));
                }
            }
        }
    }

    /// The first run's check before its change: `$ROOT/repo` doesn't exist.
    fn refuse_a_root_repo(&self) -> Result<(), ProtocolError> {
        nudge(self.root);
        let repo = self.root.join(REPO);
        match fs::symlink_metadata(&repo) {
            Err(err) if is_missing(&err) => Ok(()),
            Ok(_) => Err(ProtocolError::Other(format!(
                "{} exists next to {}: most likely an interrupted \
                 convert_to_filesystem_layout(); run it again to finish it. Nothing was \
                 changed",
                repo.display(),
                self.dir().display()
            ))),
            Err(err) => Err(to_object_store_interrupted(
                &format!("checking that {} doesn't exist", repo.display()),
                err.into(),
            )),
        }
    }

    /// Step 5: writes the version without the feature as `$ROOT/repo`,
    /// exclusively: staged as `repo.staging.<id>`, linked as `$ROOT/repo`,
    /// never renamed over an existing name. After this run's change, an
    /// existing `$ROOT/repo` other than this run's own link is an error and
    /// `repo.d` stays. A rerun keeps a `$ROOT/repo` in the object store
    /// layout; anything else stops for manual recovery. The fsyncs always
    /// run.
    fn create_object_store_repo(
        &self,
        featureless: &Featureless,
    ) -> Result<(), ProtocolError> {
        let Featureless { bytes, changed } = featureless;
        let repo = self.root.join(REPO);
        let writing = |err: io::Error| {
            to_object_store_interrupted(
                &format!("writing {}", repo.display()),
                err.into(),
            )
        };
        let staging = staging_path(&repo);
        write_fresh(&staging, bytes, true).map_err(writing)?;
        let linked = with_root_lock(self.root, || self.hooks.link(&staging, &repo));
        remove_stage(&staging);
        match linked {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists && !changed => {
                self.check_kept_root_repo()?;
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                // A lost reply of our own link: the bytes name this run's
                // fresh backup, so nothing else wrote them.
                let ours = self.read_root_repo().is_ok_and(|found| found == *bytes);
                if !ours {
                    let dir = self.dir();
                    return Err(ProtocolError::Other(format!(
                        "{repo} appeared while the conversion to the object store layout \
                         ran: something else wrote it. The conversion's change landed and \
                         {dir} was kept. Rerunning convert_to_object_store_layout() keeps \
                         {repo} if it holds a repo info in the object store layout and \
                         deletes {dir}: check {repo} first",
                        repo = repo.display(),
                        dir = dir.display(),
                    )));
                }
            }
            Err(err) => return Err(writing(err)),
        }
        fsync_dir(self.root).map_err(writing)
    }

    /// A rerun keeps an existing `$ROOT/repo` only if it is a regular file
    /// holding a repo info without the feature: this conversion's own write,
    /// or an object-store-layout write made after it. Anything else is left,
    /// with `repo.d`, for manual recovery.
    fn check_kept_root_repo(&self) -> Result<(), ProtocolError> {
        let repo = self.root.join(REPO);
        let what =
            match self.read_root_repo().map(|bytes| layout_v1::lists_feature(&bytes)) {
                Ok(Ok(false)) => return Ok(()),
                Ok(Ok(true)) => format!("lists {}", layout_v1::FEATURE),
                Ok(Err(err)) => format!("doesn't decode ({err})"),
                Err(what) => what,
            };
        let dir = self.dir();
        Err(ProtocolError::manual_recovery(
            self.root,
            &format!(
                "{repo} {what}, and a rerun of convert_to_object_store_layout() keeps only \
                 a repo info in the object store layout there. {dir} was kept. Move {repo} \
                 aside after checking that it holds nothing to keep, then run \
                 convert_to_object_store_layout() again",
                repo = repo.display(),
                dir = dir.display(),
            ),
        ))
    }

    /// The bytes of `$ROOT/repo` if it is a regular file, or why not. Reads
    /// that find it gone or stale are retried, as on a network filesystem
    /// after another client's write.
    fn read_root_repo(&self) -> Result<Bytes, String> {
        let repo = self.root.join(REPO);
        let mut last = String::new();
        with_retries(|| {
            nudge(self.root);
            let read = match fs::symlink_metadata(&repo) {
                Ok(metadata) if !metadata.is_file() => {
                    return Some(Err("isn't a regular file".to_string()));
                }
                Ok(_) => with_root_lock(self.root, || read_all(&repo)),
                Err(err) => Err(err),
            };
            match read {
                Ok(bytes) => Some(Ok(bytes)),
                Err(err) if is_missing(&err) || is_stale(&err) => {
                    last = err.to_string();
                    None
                }
                Err(err) => Some(Err(format!("can't be read ({err})"))),
            }
        })
        .unwrap_or_else(|| Err(format!("can't be read ({last})")))
    }

    /// Step 6: renames `repo.d` to a fresh `repo.d.tmp.<id>`. `false` if
    /// `repo.d` was gone and the rename wasn't ours.
    fn rename_aside(&self) -> Result<bool, ProtocolError> {
        let aside = self.root.join(tmp_dir_name(Uuid::new_v4()));
        let renaming = |err: io::Error| {
            to_object_store_interrupted("renaming repo.d aside", err.into())
        };
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
        let deleting = |err| {
            to_object_store_interrupted("deleting the repo.d.tmp.* directories", err)
        };
        self.discard_tmp_dirs().map_err(deleting)?;
        fsync_dir(self.root).map_err(|err| deleting(err.into()))?;
        self.hooks.at(to_object_store(ToObjectStoreStep::DeleteDir));
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
    info.check_writable_shape().map_err(unconvertible)?;
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

/// An I/O failure of a step of the conversion from the object store layout, as
/// an error naming the conversion to rerun.
fn interrupted(step: &str, err: ProtocolError) -> ProtocolError {
    incomplete(RERUN, step, err)
}

/// [`interrupted`], for the conversion to the object store layout.
fn to_object_store_interrupted(step: &str, err: ProtocolError) -> ProtocolError {
    incomplete(TO_OBJECT_STORE_RERUN, step, err)
}

fn incomplete(rerun: &str, step: &str, err: ProtocolError) -> ProtocolError {
    match err {
        ProtocolError::Io(err) => {
            ProtocolError::Other(format!("{step} failed: {err}; {rerun}"))
        }
        err => err,
    }
}

fn from_object_store(step: FromObjectStoreStep) -> Point {
    Point::Conversion(ConversionStep::FromObjectStore(step))
}

fn to_object_store(step: ToObjectStoreStep) -> Point {
    Point::Conversion(ConversionStep::ToObjectStore(step))
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

    // ── convert_from_object_store ───────────────────────────────────────────────

    const STEPS: [FromObjectStoreStep; 4] = [
        FromObjectStoreStep::Backup,
        FromObjectStoreStep::Build,
        FromObjectStoreStep::RenameDir,
        FromObjectStoreStep::RepoUnlinked,
    ];

    fn at_step(step: FromObjectStoreStep) -> Point {
        Point::Conversion(ConversionStep::FromObjectStore(step))
    }

    /// A repo info in the object store layout. With `num_updates`, its stored
    /// configuration sets `num_updates_per_repo_info_file`.
    fn object_store_info(num_updates: Option<u16>) -> Vec<u8> {
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
        RepoD { root, hooks }.convert_from_object_store()
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

    /// `info` stores `status` as it reads.
    fn assert_plain_status(info: &RepoInfo, status: &RepoStatus) {
        let stored = serde_json::to_value(info.raw_status().unwrap()).unwrap();
        assert_eq!(
            stored["availability"],
            format!("{:?}", status.availability),
            "{stored}"
        );
        assert_eq!(
            stored["limited_availability_reason"],
            serde_json::json!(status.limited_availability_reason),
            "{stored}"
        );
    }

    /// The end state of a conversion of `object_store_info`: `repo.d` holding
    /// its first version and marker, that version being `object_store_info`
    /// with the feature and one new `RepoStatusChangedUpdate` whose
    /// predecessor names a backup of `object_store_info`, no `$ROOT/repo`, no
    /// `repo.d.tmp.*`.
    fn assert_converted(root: &Path, object_store_info: &[u8]) -> Version {
        let Current { bytes, version } = repo(root).read_latest().unwrap().unwrap();
        assert_eq!(
            names(&root.join("repo.d")),
            set([version.file_name(), latest(&version)])
        );
        assert!(!root.join("repo").exists());
        assert_eq!(tmp_dirs(root), Vec::<String>::new());

        let before = repo_info_file::decode(object_store_info).unwrap();
        let after = repo_info_file::decode(&bytes).unwrap();
        assert!(layout_v1::lists_feature(&bytes).unwrap());
        assert_plain_status(&after, &before.status().unwrap());
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
        let attempt = layout_v1::change_info(&bytes).unwrap().attempt.unwrap().id;
        assert_eq!(backup(root, &attempt), object_store_info);
        version
    }

    #[test]
    fn convert_converts_an_object_store_repo() {
        let root = tempfile::tempdir().unwrap();
        let object_store_info = object_store_info(None);
        plant(root.path(), &object_store_info);
        let hooks = TestHooks::default();
        let before = now_ms();
        assert_eq!(
            convert(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        let after = now_ms();
        let v0 = assert_converted(root.path(), &object_store_info);
        assert!(
            (before..=after).contains(&v0.n()),
            "{} not in {before}..={after}",
            v0.n()
        );
        assert_eq!(backups(root.path()).len(), 1);
        assert_eq!(
            hooks.reached(),
            [
                at_step(FromObjectStoreStep::Backup),
                at_step(FromObjectStoreStep::Build),
                at_step(FromObjectStoreStep::RenameDir),
                at_step(FromObjectStoreStep::RepoUnlinked),
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
        let info = repo_info_file::decode(&object_store_info(None))
            .unwrap()
            .set_status(SpecVersionBin::current(), &read_only, Some(("repo.s", 10)))
            .unwrap();
        let object_store_info =
            repo_info_file::encode(&info, SpecVersionBin::current(), 3).unwrap();
        plant(root.path(), &object_store_info);
        assert_eq!(converted(root.path()), ConversionOutcome::Converted);
        assert_converted(root.path(), &object_store_info);
        let Current { bytes, .. } = repo(root.path()).read_latest().unwrap().unwrap();
        assert_eq!(repo_info_file::decode(&bytes).unwrap().status().unwrap(), read_only);
    }

    /// The ops log is paged at the repository's own configured size: with
    /// one update per file, the backup is named by `repo_before_updates`.
    #[test]
    fn convert_pages_at_the_repositorys_own_size() {
        for (num_updates, paged) in [(None, false), (Some(1), true)] {
            let root = tempfile::tempdir().unwrap();
            let object_store_info = object_store_info(num_updates);
            plant(root.path(), &object_store_info);
            assert_eq!(converted(root.path()), ConversionOutcome::Converted);
            assert_converted(root.path(), &object_store_info);
            let Current { bytes, .. } = repo(root.path()).read_latest().unwrap().unwrap();
            let named = layout_v1::backup_paths(&bytes).unwrap();
            assert_eq!(named.repo_before_updates.is_some(), paged, "{num_updates:?}");
            assert_eq!(named.entries.is_empty(), paged, "{num_updates:?}");
        }
    }

    #[test]
    fn convert_without_an_object_store_repo() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(convert(root.path(), &NoHooks).unwrap(), None);
        assert!(names(root.path()).is_empty());
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
        // It lists the feature already.
        let err = refused(&repo_info(1));
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("can't be converted")),
            "{err:?}"
        );
        let err = refused(UNDECODABLE);
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("can't be read or decoded and")),
            "{err:?}"
        );
    }

    /// Runs a conversion that crashes at `step`.
    fn crash_at(root: &Path, step: FromObjectStoreStep) {
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
            let object_store_info = object_store_info(None);
            plant(root.path(), &object_store_info);
            crash_at(root.path(), step);
            // `$ROOT/repo` holds the original repo info until the unlink.
            let repo_file = fs::read(root.path().join("repo")).ok();
            assert_eq!(
                repo_file,
                (step != FromObjectStoreStep::RepoUnlinked)
                    .then(|| object_store_info.clone()),
                "{step:?}"
            );
            assert_eq!(backups(root.path()).len(), 1, "{step:?}");
            assert_eq!(
                tmp_dirs(root.path()).len(),
                usize::from(step == FromObjectStoreStep::Build)
            );
            assert_eq!(
                root.path().join("repo.d").exists(),
                matches!(
                    step,
                    FromObjectStoreStep::RenameDir | FromObjectStoreStep::RepoUnlinked
                ),
                "{step:?}"
            );

            let expected = match step {
                FromObjectStoreStep::RepoUnlinked => ConversionOutcome::NothingToConvert,
                _ => ConversionOutcome::Converted,
            };
            assert_eq!(converted(root.path()), expected, "{step:?}");
            assert_converted(root.path(), &object_store_info);
            // A rerun before the directory rename writes another backup.
            let reran_from_the_start =
                matches!(step, FromObjectStoreStep::Backup | FromObjectStoreStep::Build);
            assert_eq!(backups(root.path()).len(), 1 + usize::from(reran_from_the_start));
        }
    }

    #[test]
    fn leftover_tmp_dirs_of_earlier_runs_are_discarded() {
        let root = tempfile::tempdir().unwrap();
        let object_store_info = object_store_info(None);
        plant(root.path(), &object_store_info);
        crash_at(root.path(), FromObjectStoreStep::Build);
        crash_at(root.path(), FromObjectStoreStep::Build);
        assert_eq!(tmp_dirs(root.path()).len(), 2);
        assert_eq!(converted(root.path()), ConversionOutcome::Converted);
        assert_converted(root.path(), &object_store_info);
    }

    /// The state a crash after the directory rename leaves, then `$ROOT/repo`
    /// changed by an object-store-layout write.
    fn interrupted_then_overwritten(root: &Path) -> Vec<u8> {
        plant(root, &object_store_info(None));
        crash_at(root, FromObjectStoreStep::RenameDir);
        let overwritten = featureless_repo_info(7);
        plant(root, &overwritten);
        overwritten
    }

    #[test]
    fn a_rerun_stops_if_repo_changed_after_the_directory_rename() {
        let root = tempfile::tempdir().unwrap();
        let overwritten = interrupted_then_overwritten(root.path());
        let before = (names(root.path()), names(&root.path().join("repo.d")));
        let err = convert(root.path(), &NoHooks).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg)
                if msg.contains("needs manual recovery")
                    && msg.contains("convert_to_filesystem_layout()")),
            "{err:?}"
        );
        assert_eq!((names(root.path()), names(&root.path().join("repo.d"))), before);
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), overwritten);
    }

    #[test]
    fn sabotage_from_object_store_without_provenance_loses_an_object_store_write() {
        let root = tempfile::tempdir().unwrap();
        interrupted_then_overwritten(root.path());
        let hooks =
            TestHooks::default().sabotage(Sabotage::FromObjectStoreWithoutProvenance);
        assert_eq!(
            convert(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        // The planted change is lost.
        assert!(!root.path().join("repo").exists());
    }

    /// A `repo.d` that step 1's listing missed appears before our directory
    /// rename: the rename fails with `EEXIST` or `ENOTEMPTY`, our tmp
    /// directory is discarded, and classifying again finds a finished
    /// conversion.
    #[test]
    fn a_repo_d_that_appears_before_the_rename_is_classified_again() {
        let root = tempfile::tempdir().unwrap();
        plant(root.path(), &object_store_info(None));
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
            at_step(FromObjectStoreStep::Build),
            |hooks| convert(root.path(), hooks).unwrap(),
            || {
                assert_eq!(tmp_dirs(root.path()).len(), 1);
                theirs = Some(repo_d_with(root.path(), &repo_info(5)));
                fs::remove_file(root.path().join("repo")).unwrap();
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
        assert!(
            !reached.contains(&at_step(FromObjectStoreStep::RenameDir)),
            "{reached:?}"
        );
        assert_eq!(tmp_dirs(root.path()), Vec::<String>::new());
        let theirs = theirs.unwrap();
        assert_eq!(
            names(&root.path().join("repo.d")),
            set([theirs.file_name(), latest(&theirs)])
        );
        assert!(!root.path().join("repo").exists());
    }

    /// An I/O failure in a step of the conversion names the conversion to
    /// rerun, and the cause; a rerun completes the conversion.
    #[test]
    fn io_failures_name_the_conversion_to_rerun() {
        let root = tempfile::tempdir().unwrap();
        let object_store_info = object_store_info(None);
        plant(root.path(), &object_store_info);
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
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), object_store_info);

        // The root becomes read-only once the backup is written.
        use std::os::unix::fs::PermissionsExt as _;
        let read_only = root.path().to_path_buf();
        let hooks = TestHooks::default().at(move |point| {
            if point == at_step(FromObjectStoreStep::Backup) {
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
        assert_converted(root.path(), &object_store_info);
    }

    /// The directory rename landed and its reply was lost: the provenance
    /// check finds this run's backup.
    #[test]
    fn a_lost_reply_of_the_directory_rename_is_resolved_by_provenance() {
        let root = tempfile::tempdir().unwrap();
        let object_store_info = object_store_info(None);
        plant(root.path(), &object_store_info);
        let hooks = lose_rename_reply_onto("repo.d");
        assert_eq!(
            convert(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert!(!hooks.reached().contains(&at_step(FromObjectStoreStep::RenameDir)));
        assert_converted(root.path(), &object_store_info);
        assert_eq!(backups(root.path()).len(), 1);
    }

    #[test]
    fn a_directory_that_keeps_vanishing_fails_after_classifying_again() {
        let root = tempfile::tempdir().unwrap();
        let object_store_info = object_store_info(None);
        plant(root.path(), &object_store_info);
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
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), object_store_info);
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
    /// older ops-log entry and unlinks `$ROOT/repo`, while it is named within
    /// three ops-log files; with one update per file, two changes push it
    /// past them.
    #[test]
    fn a_rerun_after_writers_finds_the_backup_within_three_ops_log_files() {
        for (num_updates, changes, found) in
            [(None, 3, true), (Some(1), 1, true), (Some(1), 2, false)]
        {
            let root = tempfile::tempdir().unwrap();
            let object_store_info = object_store_info(num_updates);
            plant(root.path(), &object_store_info);
            crash_at(root.path(), FromObjectStoreStep::RenameDir);
            let page = num_updates.unwrap_or(DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE);
            for _ in 0..changes {
                writer_change(root.path(), page);
            }
            assert_eq!(fs::read(root.path().join("repo")).unwrap(), object_store_info);

            let rerun = convert(root.path(), &NoHooks);
            let case = format!("{num_updates:?}, {changes}");
            if found {
                assert_eq!(rerun.unwrap(), Some(ConversionOutcome::Converted), "{case}");
                assert!(!root.path().join("repo").exists(), "{case}");
            } else {
                let err = rerun.unwrap_err();
                assert!(
                    matches!(&err, ProtocolError::Other(msg) if msg.contains("needs manual recovery")),
                    "{case}: {err:?}"
                );
                assert_eq!(
                    fs::read(root.path().join("repo")).unwrap(),
                    object_store_info,
                    "{case}"
                );
            }
        }
    }

    #[test]
    fn convert_from_object_store_unlinks_the_repo_file() {
        let root = tempfile::tempdir().unwrap();
        let object_store_info = object_store_info(None);
        plant(root.path(), &object_store_info);
        assert_eq!(converted(root.path()), ConversionOutcome::Converted);
        assert!(!root.path().join("repo").exists());
        let backups = backups(root.path());
        let [backed_up] = backups.iter().collect::<Vec<_>>()[..] else {
            panic!("one backup: {backups:?}")
        };
        assert_eq!(backup(root.path(), backed_up), object_store_info);
    }

    #[test]
    fn a_rerun_unlinks_the_original_repo_file_after_a_crash() {
        let root = tempfile::tempdir().unwrap();
        let object_store_info = object_store_info(None);
        plant(root.path(), &object_store_info);
        crash_at(root.path(), FromObjectStoreStep::RenameDir);
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), object_store_info);
        assert_eq!(converted(root.path()), ConversionOutcome::Converted);
        assert!(!root.path().join("repo").exists());
        assert_converted(root.path(), &object_store_info);
    }

    /// A `$ROOT/repo` this conversion didn't build `repo.d` from, with or
    /// without the feature, is left for manual recovery.
    #[test]
    fn a_rerun_refuses_a_foreign_repo_file() {
        for foreign in [featureless_repo_info(7), repo_info(7)] {
            let root = tempfile::tempdir().unwrap();
            plant(root.path(), &object_store_info(None));
            crash_at(root.path(), FromObjectStoreStep::RenameDir);
            plant(root.path(), &foreign);
            let before = tree(root.path());
            let err = convert(root.path(), &NoHooks).unwrap_err();
            assert!(
                matches!(&err, ProtocolError::Other(msg) if msg.contains("needs manual recovery")),
                "{err:?}"
            );
            assert_eq!(tree(root.path()), before);
            assert_eq!(fs::read(root.path().join("repo")).unwrap(), foreign);
        }
    }

    #[test]
    fn a_rerun_after_the_unlink_is_nothing_to_convert() {
        let root = tempfile::tempdir().unwrap();
        let object_store_info = object_store_info(None);
        plant(root.path(), &object_store_info);
        crash_at(root.path(), FromObjectStoreStep::RepoUnlinked);
        let before = tree(root.path());
        assert_eq!(converted(root.path()), ConversionOutcome::NothingToConvert);
        assert_eq!(tree(root.path()), before);
        assert_converted(root.path(), &object_store_info);
    }

    #[test]
    fn a_rerun_with_an_undecodable_repo_file_needs_manual_recovery() {
        for planted in [UNDECODABLE.to_vec(), Vec::new()] {
            let root = tempfile::tempdir().unwrap();
            plant(root.path(), &object_store_info(None));
            converted(root.path());
            plant(root.path(), &planted);
            let before = tree(root.path());
            let err = convert(root.path(), &NoHooks).unwrap_err();
            assert!(
                matches!(&err, ProtocolError::Other(msg)
                    if msg.contains("needs manual recovery") && msg.contains("doesn't decode")),
                "{err:?}"
            );
            assert_eq!(tree(root.path()), before);
        }
    }

    /// The current version an interrupted conversion to the object store layout
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

    // ── convert_to_object_store ─────────────────────────────────────────────────

    fn to_step(step: ToObjectStoreStep) -> Point {
        Point::Conversion(ConversionStep::ToObjectStore(step))
    }

    fn to_object_store(
        root: &Path,
        hooks: &dyn ProtocolHooks,
    ) -> Result<Option<ConversionOutcome>, ProtocolError> {
        RepoD { root, hooks }.convert_to_object_store()
    }

    fn to_object_store_converted(root: &Path) -> ConversionOutcome {
        to_object_store(root, &NoHooks).unwrap().unwrap()
    }

    /// A repository in the filesystem layout, converted from one in the object
    /// store layout whose configuration sets `num_updates`, with `changes`
    /// writer changes: its current version and bytes.
    fn in_layout(root: &Path, num_updates: Option<u16>, changes: usize) -> Current {
        plant(root, &object_store_info(num_updates));
        converted(root);
        let page = num_updates.unwrap_or(DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE);
        for _ in 0..changes {
            writer_change(root, page);
        }
        repo(root).read_latest().unwrap().unwrap()
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

    /// The end state of a conversion to the object store layout of the version
    /// with `before` bytes: no `repo.d` and no `repo.d.tmp.*`, `$ROOT/repo`
    /// that version without the feature, keeping its status,
    /// with one new `RepoStatusChangedUpdate` whose predecessor names a backup
    /// of `before`. Returns the bytes of `$ROOT/repo`.
    fn assert_object_store(root: &Path, before: &[u8]) -> Vec<u8> {
        assert!(!root.join("repo.d").exists());
        assert_eq!(tmp_dirs(root), Vec::<String>::new());
        let bytes = fs::read(root.join("repo")).unwrap();
        assert!(!layout_v1::lists_feature(&bytes).unwrap());
        let (before_info, after) = (
            repo_info_file::decode(before).unwrap(),
            repo_info_file::decode(&bytes).unwrap(),
        );
        assert_plain_status(&after, &before_info.status().unwrap());
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
        let attempt = layout_v1::change_info(&bytes).unwrap().attempt.unwrap().id;
        assert_eq!(backup(root, &attempt), before);
        bytes
    }

    #[test]
    fn to_object_store_converts_the_current_version() {
        let root = tempfile::tempdir().unwrap();
        let Current { bytes, version } = in_layout(root.path(), None, 2);
        let backups_before = backups(root.path()).len();
        let hooks = TestHooks::default();
        assert_eq!(
            to_object_store(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert_object_store(root.path(), &bytes);
        let object_store_info = fs::read(root.path().join("repo")).unwrap();
        let attempt =
            layout_v1::change_info(&object_store_info).unwrap().attempt.unwrap().id;
        assert_eq!(Hash::of(&backup(root.path(), &attempt)), *version.hash());
        assert_eq!(backups(root.path()).len(), backups_before + 1);
        assert_eq!(
            hooks.reached(),
            [
                to_step(ToObjectStoreStep::BeforeChange),
                Point::Staged,
                Point::Linked,
                Point::BeforeRename,
                Point::Renamed,
                Point::BeforeTidyUp,
                to_step(ToObjectStoreStep::Change),
                to_step(ToObjectStoreStep::RepoWritten),
                to_step(ToObjectStoreStep::RenameDir),
                to_step(ToObjectStoreStep::DeleteDir),
            ]
        );
        assert_eq!(
            repo(root.path()).open_status().unwrap(),
            OpenStatus::ObjectStoreLayout
        );

        // Done: nothing changes.
        let before = tree(root.path());
        assert_eq!(
            to_object_store_converted(root.path()),
            ConversionOutcome::NothingToConvert
        );
        assert_eq!(tree(root.path()), before);

        // And back.
        assert_eq!(converted(root.path()), ConversionOutcome::Converted);
    }

    #[test]
    fn to_object_store_pages_at_the_repositorys_own_size() {
        for (num_updates, paged) in [(None, false), (Some(1), true)] {
            let root = tempfile::tempdir().unwrap();
            let Current { bytes, .. } = in_layout(root.path(), num_updates, 1);
            assert_eq!(
                to_object_store_converted(root.path()),
                ConversionOutcome::Converted
            );
            let object_store_info = assert_object_store(root.path(), &bytes);
            let named = layout_v1::backup_paths(&object_store_info).unwrap();
            assert_eq!(named.repo_before_updates.is_some(), paged, "{num_updates:?}");
            assert_eq!(named.entries.is_empty(), paged, "{num_updates:?}");
        }
    }

    #[test]
    fn to_object_store_keeps_the_repositorys_own_status() {
        let root = tempfile::tempdir().unwrap();
        let read_only = RepoStatus {
            availability: RepoAvailability::ReadOnly,
            set_at: DateTime::from_timestamp(1_700_000_001, 0).unwrap(),
            limited_availability_reason: Some("copying".to_string()),
        };
        let info = repo_info_file::decode(&object_store_info(None))
            .unwrap()
            .set_status(SpecVersionBin::current(), &read_only, Some(("repo.s", 10)))
            .unwrap();
        plant(
            root.path(),
            &repo_info_file::encode(&info, SpecVersionBin::current(), 3).unwrap(),
        );
        converted(root.path());
        let Current { bytes, .. } = repo(root.path()).read_latest().unwrap().unwrap();
        assert_eq!(to_object_store_converted(root.path()), ConversionOutcome::Converted);
        let object_store_info = assert_object_store(root.path(), &bytes);
        assert_eq!(
            repo_info_file::decode(&object_store_info).unwrap().status().unwrap(),
            read_only
        );
    }

    #[test]
    fn to_object_store_without_a_repository_in_the_layout() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(to_object_store(root.path(), &NoHooks).unwrap(), None);
        assert!(names(root.path()).is_empty());

        // A repository in the object store layout: only leftover tmp directories
        // are deleted.
        plant(root.path(), &object_store_info(None));
        fs::create_dir_all(root.path().join(tmp_dir_name(Uuid::new_v4())).join("x"))
            .unwrap();
        assert_eq!(
            to_object_store_converted(root.path()),
            ConversionOutcome::NothingToConvert
        );
        assert_eq!(names(root.path()), strings(["repo"]));
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), object_store_info(None));

        let refused = |planted: &[u8]| {
            let root = tempfile::tempdir().unwrap();
            plant(root.path(), planted);
            let err = to_object_store(root.path(), &NoHooks).unwrap_err();
            assert_eq!(names(root.path()), strings(["repo"]));
            assert_eq!(fs::read(root.path().join("repo")).unwrap(), planted);
            err
        };
        let err = refused(UNDECODABLE);
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("can't be read or decoded and")),
            "{err:?}"
        );
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
            if point == to_step(ToObjectStoreStep::BeforeChange)
                && !changed.swap(true, Ordering::SeqCst)
            {
                writer_change(&writer_root, DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE);
            }
        });
        assert_eq!(
            to_object_store(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        let before_changes = hooks
            .reached()
            .iter()
            .filter(|point| **point == to_step(ToObjectStoreStep::BeforeChange))
            .count();
        assert_eq!(before_changes, 2, "{:?}", hooks.reached());
        // The writer's change is in the converted version.
        let object_store_info = fs::read(root.path().join("repo")).unwrap();
        let entries: Vec<_> = repo_info_file::decode(&object_store_info)
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
                return Err(io::ErrorKind::NotFound.into());
            }
            Ok(())
        });
        assert_eq!(
            to_object_store(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert!(
            hooks.observed().contains(&Observed::RenameSuperseded),
            "{:?}",
            hooks.observed()
        );
        assert!(hooks.reached().contains(&to_step(ToObjectStoreStep::Change)));
        let object_store_info = fs::read(root.path().join("repo")).unwrap();
        assert!(!layout_v1::lists_feature(&object_store_info).unwrap());
        // The conversion's own change, not the later version.
        let newest = repo_info_file::decode(&object_store_info)
            .unwrap()
            .latest_updates()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        assert!(
            matches!(newest, UpdateType::RepoStatusChangedUpdate { .. }),
            "{newest:?}"
        );
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
            to_object_store(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert!(hooks.reached().contains(&to_step(ToObjectStoreStep::RenameDir)));
        assert_object_store(root.path(), &bytes);
    }

    const TO_STEPS: [ToObjectStoreStep; 5] = [
        ToObjectStoreStep::BeforeChange,
        ToObjectStoreStep::Change,
        ToObjectStoreStep::RepoWritten,
        ToObjectStoreStep::RenameDir,
        ToObjectStoreStep::DeleteDir,
    ];

    /// Runs a conversion to the object store layout that crashes at `step`.
    fn crash_to_object_store_at(root: &Path, step: ToObjectStoreStep, hooks: TestHooks) {
        let hooks = hooks.at(move |point| {
            assert_ne!(point, to_step(step), "crash");
        });
        let crashed =
            thread::scope(|scope| scope.spawn(|| to_object_store(root, &hooks)).join());
        assert!(crashed.is_err(), "{step:?}: {crashed:?}");
    }

    /// The same end state whatever step a crash interrupts.
    #[test]
    fn a_crash_at_every_step_then_a_rerun_converts_to_object_store() {
        for step in TO_STEPS {
            let root = tempfile::tempdir().unwrap();
            let Current { bytes, .. } = in_layout(root.path(), None, 1);
            let backups_before = backups(root.path()).len();
            crash_to_object_store_at(root.path(), step, TestHooks::default());
            let repo = repo(root.path());
            let changed = step != ToObjectStoreStep::BeforeChange;
            let written = !matches!(
                step,
                ToObjectStoreStep::BeforeChange | ToObjectStoreStep::Change
            );
            match repo.read_latest().unwrap() {
                Some(Current { bytes: current, .. }) if changed => {
                    assert!(!layout_v1::lists_feature(&current).unwrap(), "{step:?}");
                }
                Some(Current { bytes: current, .. }) => assert_eq!(current, bytes),
                None => assert!(
                    matches!(
                        step,
                        ToObjectStoreStep::RenameDir | ToObjectStoreStep::DeleteDir
                    ),
                    "{step:?}"
                ),
            }
            let repo_file = fs::read(root.path().join("repo")).ok();
            assert_eq!(repo_file.is_some(), written, "{step:?}");
            assert_eq!(
                tmp_dirs(root.path()).len(),
                usize::from(step == ToObjectStoreStep::RenameDir),
                "{step:?}"
            );

            let expected = if matches!(
                step,
                ToObjectStoreStep::RenameDir | ToObjectStoreStep::DeleteDir
            ) {
                ConversionOutcome::NothingToConvert
            } else {
                ConversionOutcome::Converted
            };
            assert_eq!(to_object_store_converted(root.path()), expected, "{step:?}");
            let object_store_info = assert_object_store(root.path(), &bytes);
            if let Some(repo_file) = repo_file {
                assert_eq!(object_store_info, repo_file, "{step:?}");
            }
            // One change in all: the crashed run's, or the rerun's.
            assert_eq!(backups(root.path()).len(), backups_before + 1, "{step:?}");
        }
    }

    /// A conversion from the object store layout interrupted after its
    /// directory rename leaves `$ROOT/repo` next to `repo.d`: the conversion
    /// to the object store layout stops before changing anything.
    #[test]
    fn to_object_store_during_an_interrupted_conversion_from_object_store_is_refused() {
        let root = tempfile::tempdir().unwrap();
        plant(root.path(), &object_store_info(None));
        crash_at(root.path(), FromObjectStoreStep::RenameDir);
        let unchanged = tree(root.path());
        let err = to_object_store(root.path(), &NoHooks).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg) if msg.contains("convert_to_filesystem_layout()")),
            "{err:?}"
        );
        assert_eq!(tree(root.path()), unchanged);

        assert_eq!(converted(root.path()), ConversionOutcome::Converted);
        let Current { bytes, .. } = repo(root.path()).read_latest().unwrap().unwrap();
        assert_eq!(to_object_store_converted(root.path()), ConversionOutcome::Converted);
        assert_object_store(root.path(), &bytes);
    }

    /// Whatever `$ROOT/repo` holds next to a `repo.d` that lists the feature,
    /// the first run changes nothing.
    #[test]
    fn to_object_store_refuses_a_repo_file_next_to_repo_d() {
        for planted in [Some(object_store_info(None)), None] {
            let root = tempfile::tempdir().unwrap();
            let Current { bytes, .. } = in_layout(root.path(), None, 1);
            let planted = planted.unwrap_or_else(|| bytes.to_vec());
            plant(root.path(), &planted);
            let before = tree(root.path());
            let hooks = TestHooks::default();
            let err = to_object_store(root.path(), &hooks).unwrap_err();
            assert!(
                matches!(&err, ProtocolError::Other(msg)
                    if msg.contains("convert_to_filesystem_layout()")
                        && msg.contains(&root.path().join("repo").display().to_string())),
                "{err:?}"
            );
            assert!(!hooks.reached().contains(&Point::Staged));
            assert_eq!(tree(root.path()), before);
        }
    }

    /// A crash after the conversion wrote `$ROOT/repo`, then something else
    /// replaced it: the rerun keeps it as it is and deletes `repo.d`.
    #[test]
    fn a_rerun_keeps_an_existing_repo_file() {
        let root = tempfile::tempdir().unwrap();
        in_layout(root.path(), None, 1);
        crash_to_object_store_at(
            root.path(),
            ToObjectStoreStep::RepoWritten,
            TestHooks::default(),
        );
        let foreign = featureless_repo_info(9);
        plant(root.path(), &foreign);
        assert_eq!(to_object_store_converted(root.path()), ConversionOutcome::Converted);
        assert_eq!(fs::read(root.path().join("repo")).unwrap(), foreign);
        assert!(!root.path().join("repo.d").exists());
        assert_eq!(tmp_dirs(root.path()), Vec::<String>::new());
    }

    /// A crash after the conversion wrote `$ROOT/repo`, then something
    /// replaced it with anything but a repo info without the feature: the
    /// rerun leaves it and `repo.d` for manual recovery, and finishes once it
    /// is moved aside.
    #[test]
    fn a_rerun_refuses_a_repo_file_that_is_no_object_store_repo_info() {
        enum Planted {
            Bytes(Vec<u8>),
            Dir,
        }
        for planted in [
            Planted::Bytes(UNDECODABLE.to_vec()),
            Planted::Bytes(Vec::new()),
            Planted::Bytes(repo_info(9)),
            Planted::Dir,
        ] {
            let root = tempfile::tempdir().unwrap();
            let Current { bytes, .. } = in_layout(root.path(), None, 1);
            crash_to_object_store_at(
                root.path(),
                ToObjectStoreStep::RepoWritten,
                TestHooks::default(),
            );
            let repo_file = root.path().join("repo");
            fs::remove_file(&repo_file).unwrap();
            match &planted {
                Planted::Bytes(planted) => plant(root.path(), planted),
                Planted::Dir => fs::create_dir_all(&repo_file).unwrap(),
            }
            let before = tree(root.path());
            let err = to_object_store(root.path(), &NoHooks).unwrap_err();
            assert!(
                matches!(&err, ProtocolError::Other(msg)
                    if msg.contains("needs manual recovery")
                        && msg.contains(&repo_file.display().to_string())),
                "{err:?}"
            );
            assert_eq!(tree(root.path()), before);
            assert!(root.path().join("repo.d").exists());

            match planted {
                Planted::Bytes(_) => fs::remove_file(&repo_file).unwrap(),
                Planted::Dir => fs::remove_dir(&repo_file).unwrap(),
            }
            assert_eq!(
                to_object_store_converted(root.path()),
                ConversionOutcome::Converted
            );
            assert_object_store(root.path(), &bytes);
        }
    }

    /// The first run's link of `$ROOT/repo` landed and its reply was lost:
    /// the file holds this run's bytes, and the conversion completes.
    #[test]
    fn a_lost_reply_of_the_first_runs_link_is_ours() {
        let root = tempfile::tempdir().unwrap();
        let Current { bytes, .. } = in_layout(root.path(), None, 1);
        let lost = AtomicBool::new(false);
        let hooks = TestHooks::default().link(move |from, to| {
            fs::hard_link(from, to)?;
            if to.file_name().is_some_and(|n| n == "repo")
                && !lost.swap(true, Ordering::SeqCst)
            {
                Err(io::ErrorKind::AlreadyExists.into())
            } else {
                Ok(())
            }
        });
        assert_eq!(
            to_object_store(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert!(hooks.reached().contains(&to_step(ToObjectStoreStep::RepoWritten)));
        assert_object_store(root.path(), &bytes);
    }

    /// `$ROOT/repo` appears after the first run checked its absence: the
    /// exclusive write fails and `repo.d` stays.
    #[test]
    fn a_first_run_whose_repo_file_appears_after_the_check_fails() {
        let root = tempfile::tempdir().unwrap();
        in_layout(root.path(), None, 1);
        let repo_file = root.path().join("repo");
        let foreign = featureless_repo_info(9);
        let (at_change, to_plant) = (repo_file.clone(), foreign.clone());
        let hooks = TestHooks::default().at(move |point| {
            if point == to_step(ToObjectStoreStep::Change) {
                fs::write(&at_change, &to_plant).unwrap();
            }
        });
        let err = to_object_store(root.path(), &hooks).unwrap_err();
        assert!(
            matches!(&err, ProtocolError::Other(msg)
                if msg.contains(&repo_file.display().to_string())
                    && msg.contains("deletes")),
            "{err:?}"
        );
        assert!(!hooks.reached().contains(&to_step(ToObjectStoreStep::RepoWritten)));
        assert_eq!(fs::read(&repo_file).unwrap(), foreign);
        assert!(root.path().join("repo.d").exists());
    }

    /// A change lands after the version the conversion read: without its own
    /// change, the conversion writes the stale version, and the
    /// acknowledged change is lost.
    #[test]
    fn sabotage_to_object_store_without_change_loses_an_acknowledged_change() {
        let root = tempfile::tempdir().unwrap();
        let Current { bytes, .. } = in_layout(root.path(), None, 0);
        let writer_root = root.path().to_path_buf();
        let changed = AtomicBool::new(false);
        let hooks = TestHooks::default()
            .sabotage(Sabotage::ToObjectStoreWithoutChange)
            .at(move |point| {
                if point == to_step(ToObjectStoreStep::BeforeChange)
                    && !changed.swap(true, Ordering::SeqCst)
                {
                    writer_change(&writer_root, DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE);
                }
            });
        assert_eq!(
            to_object_store(root.path(), &hooks).unwrap(),
            Some(ConversionOutcome::Converted)
        );
        assert!(!hooks.reached().contains(&Point::Staged));
        // The stale version, without the writer's change.
        let object_store_info = assert_object_store(root.path(), &bytes);
        let entries: Vec<_> = repo_info_file::decode(&object_store_info)
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

    /// `repo.d` deleted before `$ROOT/repo` is written, then a crash: neither
    /// is left, and the repository is lost.
    #[test]
    fn sabotage_delete_dir_before_repo_write_loses_the_repository() {
        let root = tempfile::tempdir().unwrap();
        in_layout(root.path(), None, 1);
        let hooks = TestHooks::default().sabotage(Sabotage::DeleteDirBeforeRepoWrite);
        crash_to_object_store_at(root.path(), ToObjectStoreStep::DeleteDir, hooks);
        assert!(!root.path().join("repo.d").exists());
        assert!(!root.path().join("repo").exists());
        assert_eq!(repo(root.path()).open_status().unwrap(), OpenStatus::NotFound);
        assert_eq!(to_object_store(root.path(), &NoHooks).unwrap(), None);
    }
}
