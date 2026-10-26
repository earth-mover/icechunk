//! Filesystem layout v1, the `repo.d` layout of the repo info, see the
//! filesystem layout v1 page of the specification.

use chrono::{DateTime, Utc};

use crate::{
    IcechunkResult,
    envelope::DEFAULT_COMPRESSION_LEVEL,
    repo_info::{RepoInfo, UpdateType},
    repo_info_file::{decode, encode},
    writer_features::{FeatureName, WriterFeature},
};

/// The writer feature a repo info in this layout lists.
pub const FEATURE: &str = "filesystem-layout-v1";

/// Whether [`convert`] adds or removes [`FEATURE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureChange {
    Add,
    Remove,
}

/// The backup paths one repo info file contains, see [`backup_paths`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackupPaths {
    /// The `backup_path` of every ops-log entry that has one, newest first.
    pub entries: Vec<String>,
    /// Where the ops log continues: the `backup_path` of the newest entry
    /// paged out of this file.
    pub repo_before_updates: Option<String>,
}

/// Whether the repo info lists [`FEATURE`].
pub fn lists_feature(bytes: &[u8]) -> IcechunkResult<bool> {
    lists(&decode(bytes)?)
}

/// The attempt that wrote a repo info.
///
/// Before replacing a version, core copies it to `overwritten/<path>`, with a
/// path it draws anew for every attempt, and records that path on the
/// replaced version's ops-log entry. So the path identifies the attempt: the
/// ops log of the current version points to it if and only if the attempt landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attempt {
    /// The backup path on the second newest ops-log entry. When paging moved
    /// that entry to an older file, the same path is this file's
    /// `repo_before_updates`.
    pub id: String,
    /// The id of the attempt that wrote the version this one replaced, `None`
    /// if this file doesn't name it. A landed `id` sits right above `base_id`.
    pub base_id: Option<String>,
}

/// What a change needs to know about its new repo info, see [`change_info`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeInfo {
    /// Whether it lists [`FEATURE`].
    pub lists_feature: bool,
    /// `None` when the replaced version's entry records no backup path, as
    /// for a first version.
    pub attempt: Option<Attempt>,
}

/// [`lists_feature`] and the [`Attempt`] of a repo info, decoding the bytes
/// once.
pub fn change_info(bytes: &[u8]) -> IcechunkResult<ChangeInfo> {
    let info = decode(bytes)?;
    Ok(ChangeInfo { lists_feature: lists(&info)?, attempt: attempt(&info)? })
}

fn attempt(info: &RepoInfo) -> IcechunkResult<Option<Attempt>> {
    let mut entries = info.latest_updates()?;
    // The newest entry is this update's own.
    entries.next().transpose()?;
    let id = match entries.next().transpose()? {
        Some((_, _, backup_path)) => backup_path.map(str::to_string),
        None => info.repo_before_updates()?.map(str::to_string),
    };
    let Some(id) = id else {
        return Ok(None);
    };
    let named = backup_paths_of(info)?;
    let base_id = named
        .entries
        .into_iter()
        .chain(named.repo_before_updates)
        .skip_while(|path| *path != id)
        .nth(1);
    Ok(Some(Attempt { id, base_id }))
}

/// The backup paths this one file names: those of its ops-log entries and its
/// `repo_before_updates`. No I/O: to search older entries, read the file
/// `repo_before_updates` names and call this again.
pub fn backup_paths(bytes: &[u8]) -> IcechunkResult<BackupPaths> {
    backup_paths_of(&decode(bytes)?)
}

fn backup_paths_of(info: &RepoInfo) -> IcechunkResult<BackupPaths> {
    let mut entries = Vec::new();
    for entry in info.latest_updates()? {
        if let (_, _, Some(backup_path)) = entry? {
            entries.push(backup_path.to_string());
        }
    }
    Ok(BackupPaths {
        entries,
        repo_before_updates: info.repo_before_updates()?.map(str::to_string),
    })
}

/// Whether the repo info is what an interrupted creation leaves: it lists
/// [`FEATURE`] and its whole ops log is one `RepoInitializedUpdate`.
pub fn is_interrupted_creation(bytes: &[u8]) -> IcechunkResult<bool> {
    let info = decode(bytes)?;
    if !lists(&info)? || info.repo_before_updates()?.is_some() {
        return Ok(false);
    }
    let mut entries = info.latest_updates()?;
    let first = entries.next().transpose()?;
    let only_initialized = matches!(first, Some((UpdateType::RepoInitializedUpdate, ..)))
        && entries.next().transpose()?.is_none();
    Ok(only_initialized)
}

/// The repo info file with [`FEATURE`] added or removed, keeping the status,
/// and a `RepoStatusChangedUpdate` at `now` whose predecessor entry names
/// `backup_path`, the ops log paged at `num_updates_per_file`.
///
/// `now` must be later than the newest entry's `updated_at`: callers raise
/// their clock above it when it lags. Callers also check, before calling, that
/// they implement every other listed feature and that the repo info is well
/// formed: this function doesn't. Adding a listed feature or removing an
/// unlisted one is an error.
pub fn convert(
    bytes: &[u8],
    change: FeatureChange,
    backup_path: &str,
    now: DateTime<Utc>,
    num_updates_per_file: u16,
) -> IcechunkResult<Vec<u8>> {
    let info = decode(bytes)?;
    let spec_version = info.spec_version()?;
    let feature = [WriterFeature::FilesystemLayoutV1];
    let (add, remove): (&[WriterFeature], &[WriterFeature]) = match change {
        FeatureChange::Add => (&feature, &[]),
        FeatureChange::Remove => (&[], &feature),
    };
    let converted = info.with_writer_features(
        spec_version,
        add,
        remove,
        backup_path,
        now,
        num_updates_per_file,
    )?;
    encode(&converted, spec_version, DEFAULT_COMPRESSION_LEVEL)
}

fn lists(info: &RepoInfo) -> IcechunkResult<bool> {
    Ok(info.writer_features()?.contains(&FeatureName::new(FEATURE)))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;
    use crate::{
        IcechunkFormatErrorKind, SnapshotId,
        format_constants::SpecVersionBin,
        repo_info::{
            DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE, RepoAvailability, RepoStatus,
        },
        snapshot::SnapshotInfo,
    };

    type TestResult = Result<(), Box<dyn Error>>;

    fn at(seconds: i64) -> Result<DateTime<Utc>, Box<dyn Error>> {
        Ok(DateTime::from_timestamp(1_700_000_000 + seconds, 0).ok_or("bad time")?)
    }

    fn initial(features: &[WriterFeature]) -> Result<RepoInfo, Box<dyn Error>> {
        let time = at(0)?;
        Ok(RepoInfo::initial(
            SpecVersionBin::current(),
            SnapshotInfo {
                id: SnapshotId::random(),
                parent_id: None,
                flushed_at: time,
                message: "initial".to_string(),
                metadata: Default::default(),
                pruned_ancestor_tx_logs: vec![],
            },
            DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE,
            None::<&()>,
            Some(time),
            features,
        ))
    }

    fn file(info: &RepoInfo) -> Result<Vec<u8>, Box<dyn Error>> {
        Ok(encode(info, SpecVersionBin::current(), 3)?)
    }

    fn with_feature() -> Result<Vec<u8>, Box<dyn Error>> {
        file(&initial(&[WriterFeature::FilesystemLayoutV1])?)
    }

    fn featureless() -> Result<Vec<u8>, Box<dyn Error>> {
        file(&initial(&[])?)
    }

    fn convert_default(
        bytes: &[u8],
        change: FeatureChange,
        backup_path: &str,
        seconds: i64,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        Ok(convert(
            bytes,
            change,
            backup_path,
            at(seconds)?,
            DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE,
        )?)
    }

    fn malformed() -> Vec<Vec<u8>> {
        vec![Vec::new(), b"not a repo info".to_vec(), vec![0; 64]]
    }

    #[icechunk_macros::test]
    fn the_feature_is_the_writer_feature_filesystem_layout_v1() {
        assert_eq!(FEATURE, "filesystem-layout-v1");
        assert_eq!(
            WriterFeature::from_name(FEATURE),
            Some(WriterFeature::FilesystemLayoutV1)
        );
    }

    #[icechunk_macros::test]
    fn lists_feature_reads_the_writer_features() -> TestResult {
        assert!(lists_feature(&with_feature()?)?);
        assert!(!lists_feature(&featureless()?)?);
        Ok(())
    }

    #[icechunk_macros::test]
    fn the_attempt_of_a_paged_predecessor_is_repo_before_updates() -> TestResult {
        let paged =
            convert(&with_feature()?, FeatureChange::Remove, "repo.a", at(1)?, 1)?;
        let attempt = change_info(&paged)?.attempt.ok_or("no attempt")?;
        assert_eq!(attempt.id, "repo.a");
        assert_eq!(
            backup_paths(&paged)?,
            BackupPaths {
                entries: vec![],
                repo_before_updates: Some("repo.a".to_string())
            }
        );
        Ok(())
    }

    #[icechunk_macros::test]
    fn backup_paths_lists_the_entries_newest_first() -> TestResult {
        assert_eq!(backup_paths(&with_feature()?)?, BackupPaths::default());
        let removed =
            convert_default(&with_feature()?, FeatureChange::Remove, "repo.a", 1)?;
        let added = convert_default(&removed, FeatureChange::Add, "repo.b", 2)?;
        assert_eq!(
            backup_paths(&added)?,
            BackupPaths {
                entries: vec!["repo.b".to_string(), "repo.a".to_string()],
                repo_before_updates: None
            }
        );
        let paged = convert(&added, FeatureChange::Remove, "repo.c", at(3)?, 2)?;
        assert_eq!(
            backup_paths(&paged)?,
            BackupPaths {
                entries: vec!["repo.c".to_string()],
                repo_before_updates: Some("repo.b".to_string())
            }
        );
        Ok(())
    }

    #[icechunk_macros::test]
    fn change_info_is_the_feature_and_the_attempt() -> TestResult {
        let info = |lists_feature, id: Option<(&str, Option<&str>)>| ChangeInfo {
            lists_feature,
            attempt: id.map(|(id, base_id)| Attempt {
                id: id.to_string(),
                base_id: base_id.map(str::to_string),
            }),
        };
        assert_eq!(change_info(&with_feature()?)?, info(true, None));
        let removed =
            convert_default(&with_feature()?, FeatureChange::Remove, "repo.a", 1)?;
        assert_eq!(change_info(&removed)?, info(false, Some(("repo.a", None))));
        let added = convert_default(&removed, FeatureChange::Add, "repo.b", 2)?;
        assert_eq!(change_info(&added)?, info(true, Some(("repo.b", Some("repo.a")))));
        let paged = convert(&added, FeatureChange::Remove, "repo.c", at(3)?, 2)?;
        assert_eq!(change_info(&paged)?, info(false, Some(("repo.c", Some("repo.b")))));
        let all_paged = convert(&added, FeatureChange::Remove, "repo.c", at(3)?, 1)?;
        assert_eq!(change_info(&all_paged)?, info(false, Some(("repo.c", None))));
        Ok(())
    }

    #[icechunk_macros::test]
    fn only_a_first_version_with_the_feature_is_an_interrupted_creation() -> TestResult {
        assert!(is_interrupted_creation(&with_feature()?)?);
        assert!(!is_interrupted_creation(&featureless()?)?);
        let removed =
            convert_default(&with_feature()?, FeatureChange::Remove, "repo.a", 1)?;
        let added = convert_default(&removed, FeatureChange::Add, "repo.b", 2)?;
        assert!(lists_feature(&added)?);
        assert!(!is_interrupted_creation(&added)?);
        // Paged so that the only entry left is the newest one.
        let paged = convert(&removed, FeatureChange::Add, "repo.b", at(2)?, 1)?;
        assert!(!is_interrupted_creation(&paged)?);
        Ok(())
    }

    #[icechunk_macros::test]
    fn convert_changes_only_the_feature_the_gate_and_the_ops_log() -> TestResult {
        let read_only = RepoStatus {
            availability: RepoAvailability::ReadOnly,
            set_at: at(1)?,
            limited_availability_reason: Some("copying".to_string()),
        };
        let original = initial(&[])?.set_status(
            SpecVersionBin::current(),
            &read_only,
            Some(("repo.s", DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE)),
        )?;
        // Whole seconds, as stored; after the status change's time.
        let now =
            DateTime::from_timestamp(Utc::now().timestamp() + 2, 0).ok_or("bad time")?;
        let added = decode(&convert(
            &file(&original)?,
            FeatureChange::Add,
            "repo.a",
            now,
            DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE,
        )?)?;
        assert_eq!(added.writer_features()?, [FeatureName::new(FEATURE)].into());
        assert_eq!(added.status()?, read_only);
        assert_eq!(
            added.raw_status()?.repo_status().availability,
            RepoAvailability::ReadOnly
        );
        let entries: Vec<_> = added.latest_updates()?.collect::<Result<_, _>>()?;
        assert!(
            matches!(&entries[0], (UpdateType::RepoStatusChangedUpdate { .. }, time, None) if *time == now),
            "{entries:?}"
        );
        assert_eq!(entries[1].2, Some("repo.a"));
        assert_eq!(entries.len(), 3);
        assert_eq!(
            added.branches()?.collect::<Vec<_>>(),
            original.branches()?.collect::<Vec<_>>()
        );

        let removed = decode(&convert(
            &file(&added)?,
            FeatureChange::Remove,
            "repo.b",
            now + chrono::Duration::seconds(1),
            DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE,
        )?)?;
        assert!(removed.writer_features()?.is_empty());
        assert_eq!(removed.status()?, read_only);
        Ok(())
    }

    #[icechunk_macros::test]
    fn convert_refuses_a_change_that_changes_nothing() -> TestResult {
        for (bytes, change) in [
            (with_feature()?, FeatureChange::Add),
            (featureless()?, FeatureChange::Remove),
        ] {
            let err = convert(&bytes, change, "repo.a", at(1)?, 10)
                .err()
                .ok_or("a no-op conversion must fail")?;
            assert!(
                matches!(
                    err.kind,
                    IcechunkFormatErrorKind::InvalidWriterFeatureChange(_)
                ),
                "{err}"
            );
        }
        Ok(())
    }

    #[icechunk_macros::test]
    fn convert_needs_a_time_after_the_newest_entry() -> TestResult {
        let err = convert(&featureless()?, FeatureChange::Add, "repo.a", at(0)?, 10)
            .err()
            .ok_or("an update at the newest entry's time must fail")?;
        assert!(
            matches!(err.kind, IcechunkFormatErrorKind::InvalidUpdateTimestamp { .. }),
            "{err}"
        );
        Ok(())
    }

    #[icechunk_macros::test]
    fn malformed_bytes_are_format_errors() -> TestResult {
        for bytes in malformed() {
            lists_feature(&bytes).err().ok_or("lists_feature")?;
            change_info(&bytes).err().ok_or("change_info")?;
            backup_paths(&bytes).err().ok_or("backup_paths")?;
            is_interrupted_creation(&bytes).err().ok_or("is_interrupted_creation")?;
            convert(&bytes, FeatureChange::Add, "repo.a", at(1)?, 10)
                .err()
                .ok_or("convert")?;
        }
        Ok(())
    }
}
