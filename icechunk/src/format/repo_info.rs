//! Version info, branches, and tags for a repository.

use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
};
use tracing::trace;

use crate::{config::RepositoryConfig, format::snapshot::SnapshotProperties, refs::Ref};

use super::{
    IcechunkFormatError, IcechunkFormatErrorKind, IcechunkResult, SnapshotId,
    flatbuffers::generated, format_constants::SpecVersionBin, lookup_index_by_key,
    snapshot::SnapshotInfo,
};

use chrono::{DateTime, Utc};
use flatbuffers::{UnionWIPOffset, VerifierOptions, WIPOffset};
use icechunk_types::ICResultExt as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepoAvailability {
    Online,
    ReadOnly,
    // Offline is defined in the flatbuffers, but we won't support it
    // before better specs on how we want to use it
}

impl From<generated::RepoAvailability> for RepoAvailability {
    fn from(value: generated::RepoAvailability) -> Self {
        match value {
            generated::RepoAvailability::Online => RepoAvailability::Online,
            generated::RepoAvailability::ReadOnly => RepoAvailability::ReadOnly,
            _ => RepoAvailability::Online,
        }
    }
}

impl From<RepoAvailability> for generated::RepoAvailability {
    fn from(value: RepoAvailability) -> Self {
        match value {
            RepoAvailability::Online => generated::RepoAvailability::Online,
            RepoAvailability::ReadOnly => generated::RepoAvailability::ReadOnly,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoStatus {
    pub availability: RepoAvailability,
    pub set_at: DateTime<Utc>,
    pub limited_availability_reason: Option<String>,
}

impl TryFrom<generated::RepoStatus<'_>> for RepoStatus {
    type Error = IcechunkFormatError;

    fn try_from(fb_status: generated::RepoStatus<'_>) -> Result<Self, Self::Error> {
        let ts: i64 = fb_status
            .set_at()
            .try_into()
            .map_err(|_| IcechunkFormatErrorKind::InvalidTimestamp)
            .capture()?;
        let set_at = DateTime::from_timestamp_micros(ts)
            .ok_or(IcechunkFormatErrorKind::InvalidTimestamp)
            .capture()?;
        Ok(RepoStatus {
            availability: fb_status.availability().into(),
            set_at,
            limited_availability_reason: fb_status
                .limited_availability_reason()
                .map(|s| s.to_string()),
        })
    }
}

impl RepoStatus {
    pub(crate) fn error_msg(&self) -> String {
        format!(
            "Repo status is {0:?}, set at {1}, reason: {2:?}",
            self.availability, self.set_at, self.limited_availability_reason
        )
    }
}

// TODO: should we not implement serialize and let the session fetch the repo info?
#[derive(PartialEq, Serialize, Deserialize)]
pub struct RepoInfo {
    buffer: Vec<u8>,
}

impl std::fmt::Debug for RepoInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tags = self.tags().map(Vec::from_iter).unwrap_or_default();
        let tags =
            tags.into_iter().map(|(name, snap)| format!("{name} -> {snap}")).join(", ");
        let branches = self.branches().map(Vec::from_iter).unwrap_or_default();
        let branches = branches
            .into_iter()
            .map(|(name, snap)| format!("{name} -> {snap}"))
            .join(", ");
        let snaps = self.all_snapshots().map(Vec::from_iter).unwrap_or_default();
        let snaps = snaps
            .into_iter()
            .map(|ms| match ms {
                Ok(snap) => format!(
                    "{} -> {}",
                    snap.id,
                    snap.parent_id.map(|s| s.to_string()).unwrap_or_default()
                ),
                Err(_) => "#err".to_string(),
            })
            .join(", ");
        // FIXME: add other fields
        f.debug_struct("RepoInfo")
            .field("tags", &tags)
            .field("branches", &branches)
            .field("snapshots", &snaps)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateType {
    RepoInitializedUpdate,
    RepoMigratedUpdate {
        from_version: SpecVersionBin,
        to_version: SpecVersionBin,
    },
    RepoStatusChangedUpdate {
        status: RepoStatus,
    },
    ConfigChangedUpdate,
    MetadataChangedUpdate,
    TagCreatedUpdate {
        name: String,
    },
    TagDeletedUpdate {
        name: String,
        previous_snap_id: SnapshotId,
    },
    BranchCreatedUpdate {
        name: String,
    },
    BranchDeletedUpdate {
        name: String,
        previous_snap_id: SnapshotId,
    },
    BranchResetUpdate {
        name: String,
        previous_snap_id: SnapshotId,
    },
    NewCommitUpdate {
        branch: String,
        new_snap_id: SnapshotId,
    },
    CommitAmendedUpdate {
        branch: String,
        previous_snap_id: SnapshotId,
        new_snap_id: SnapshotId,
    },
    NewDetachedSnapshotUpdate {
        new_snap_id: SnapshotId,
    },
    GCRanUpdate,
    ExpirationRanUpdate,
    FeatureFlagChanged {
        id: u16,
        new_value: Option<bool>,
    },
}

static ROOT_OPTIONS: VerifierOptions = VerifierOptions {
    max_depth: 10,
    max_tables: 5_000_000,
    max_apparent_size: 1 << 31, // taken from the default
    ignore_missing_null_terminator: true,
};

#[derive(Debug, Clone)]
pub struct UpdateInfo<I> {
    pub update_type: UpdateType,
    pub update_time: DateTime<Utc>,
    pub previous_updates: I,
}

type UpdateTuple<'a> = IcechunkResult<(UpdateType, DateTime<Utc>, Option<&'a str>)>;

impl RepoInfo {
    #[expect(clippy::too_many_arguments)]
    pub fn new<
        'a,
        I: IntoIterator<Item = IcechunkResult<(UpdateType, DateTime<Utc>, Option<&'a str>)>>,
        EFFIt: DoubleEndedIterator<Item = u16> + ExactSizeIterator,
        DFFIt: DoubleEndedIterator<Item = u16> + ExactSizeIterator,
    >(
        spec_version: SpecVersionBin,
        tags: impl IntoIterator<Item = (&'a str, SnapshotId)>,
        branches: impl IntoIterator<Item = (&'a str, SnapshotId)>,
        deleted_tags: impl IntoIterator<Item = &'a str>,
        snapshots: impl IntoIterator<Item = SnapshotInfo>,
        metadata: &SnapshotProperties,
        update: UpdateInfo<I>,
        backup_path: Option<&'a str>,
        num_updates_per_file: u16,
        previous_info: Option<&'a str>,
        config_bytes: Option<&[u8]>,
        sorted_enabled_feature_flags: Option<EFFIt>,
        sorted_disabled_feature_flags: Option<DFFIt>,
        status: &RepoStatus,
    ) -> IcechunkResult<Self> {
        let mut snapshots: Vec<_> = snapshots.into_iter().collect();
        snapshots.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        let tags = resolve_ref_iter(&snapshots, tags)?;
        let branches = resolve_ref_iter(&snapshots, branches)?;
        let mut deleted_tags: Vec<_> = deleted_tags.into_iter().collect();
        deleted_tags.sort();
        Self::from_parts(
            spec_version,
            tags,
            branches,
            deleted_tags,
            snapshots,
            metadata,
            update,
            backup_path,
            num_updates_per_file,
            previous_info,
            config_bytes,
            sorted_enabled_feature_flags,
            sorted_disabled_feature_flags,
            status,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn from_parts<
        'a,
        I: IntoIterator<Item = IcechunkResult<(UpdateType, DateTime<Utc>, Option<&'a str>)>>,
        EFFIt: DoubleEndedIterator<Item = u16> + ExactSizeIterator,
        DFFIt: DoubleEndedIterator<Item = u16> + ExactSizeIterator,
    >(
        spec_version: SpecVersionBin,
        sorted_tags: impl IntoIterator<Item = (&'a str, u32)>,
        sorted_branches: impl IntoIterator<Item = (&'a str, u32)>,
        sorted_deleted_tags: impl IntoIterator<Item = &'a str>,
        sorted_snapshots: impl IntoIterator<Item = SnapshotInfo>,
        metadata: &SnapshotProperties,
        update: UpdateInfo<I>,
        backup_path: Option<&'a str>,
        num_updates_per_file: u16,
        previous_info: Option<&'a str>,
        config_bytes: Option<&[u8]>,
        sorted_enabled_feature_flags: Option<EFFIt>,
        sorted_disabled_feature_flags: Option<DFFIt>,
        status: &RepoStatus,
    ) -> IcechunkResult<Self> {
        trace!("Creating new repo info from parts");
        let mut builder = flatbuffers::FlatBufferBuilder::with_capacity(4_096);
        let tags = sorted_tags
            .into_iter()
            .map(|(name, offset)| {
                let args = generated::RefArgs {
                    name: Some(builder.create_string(name)),
                    snapshot_index: offset,
                };
                generated::Ref::create(&mut builder, &args)
            })
            .collect::<Vec<_>>();
        let tags = builder.create_vector(&tags);

        let mut main_found = false;
        let branches = sorted_branches
            .into_iter()
            .map(|(name, offset)| {
                if name == Ref::DEFAULT_BRANCH {
                    main_found = true;
                }
                let args = generated::RefArgs {
                    name: Some(builder.create_string(name)),
                    snapshot_index: offset,
                };
                generated::Ref::create(&mut builder, &args)
            })
            .collect::<Vec<_>>();
        if !main_found {
            return Err(IcechunkFormatErrorKind::BranchNotFound {
                branch: Ref::DEFAULT_BRANCH.to_string(),
            })
            .capture();
        }

        let branches = builder.create_vector(&branches);

        let deleted_tags = sorted_deleted_tags
            .into_iter()
            .map(|name| builder.create_string(name))
            .collect::<Vec<_>>();
        let deleted_tags = builder.create_vector(&deleted_tags);

        let snapshots: Vec<_> = sorted_snapshots.into_iter().collect();
        debug_assert!(snapshots.is_sorted_by(|a, b| a.id.0 <= b.id.0));

        let snapshot_index: HashMap<_, _> =
            snapshots.iter().enumerate().map(|(ix, sn)| (sn.id.clone(), ix)).collect();

        // TODO: we should check no loops
        let snapshots: Vec<_> = snapshots
            .iter()
            .map(|snap| {
                let id = &snap.id.0;
                let id = generated::ObjectId12::new(id);
                let parent_offset = match snap.parent_id.as_ref() {
                    Some(parent_id) => {
                        let index = snapshot_index
                            .get(parent_id)
                            .ok_or_else(|| IcechunkFormatErrorKind::SnapshotIdNotFound {
                                snapshot_id: snap.id.clone(),
                            })
                            .capture()?;
                        Ok(*index as i32)
                    }
                    None => Ok::<_, IcechunkFormatError>(-1),
                }?;

                let metadata_items: Vec<_> = snap
                    .metadata
                    .iter()
                    .map(|(k, v)| {
                        let name = builder.create_shared_string(k.as_str());
                        let serialized =
                            flexbuffers::to_vec(v).map_err(Box::new).capture()?;
                        let value = builder.create_vector(serialized.as_slice());
                        let item = generated::MetadataItem::create(
                            &mut builder,
                            &generated::MetadataItemArgs {
                                name: Some(name),
                                value: Some(value),
                            },
                        );
                        Ok::<_, IcechunkFormatError>(item)
                    })
                    .try_collect()?;

                let metadata = builder.create_vector(metadata_items.as_slice());
                let args = generated::SnapshotInfoArgs {
                    id: Some(&id),
                    parent_offset,
                    flushed_at: snap.flushed_at.timestamp_micros() as u64,
                    message: Some(builder.create_string(snap.message.as_str())),
                    metadata: Some(metadata),
                };
                Ok::<_, IcechunkFormatError>(generated::SnapshotInfo::create(
                    &mut builder,
                    &args,
                ))
            })
            .try_collect()?;
        let snapshots = builder.create_vector(&snapshots);

        let limited_reason = status
            .limited_availability_reason
            .as_deref()
            .map(|s| builder.create_string(s));
        let status = generated::RepoStatus::create(
            &mut builder,
            &generated::RepoStatusArgs {
                availability: status.availability.into(),
                set_at: status.set_at.timestamp_micros() as u64,
                limited_availability_reason: limited_reason,
            },
        );

        let metadata_items: Vec<_> = metadata
            .iter()
            .map(|(k, v)| {
                let name = builder.create_shared_string(k.as_str());
                let serialized = flexbuffers::to_vec(v).map_err(Box::new).capture()?;
                let value = builder.create_vector(serialized.as_slice());
                let item = generated::MetadataItem::create(
                    &mut builder,
                    &generated::MetadataItemArgs { name: Some(name), value: Some(value) },
                );
                Ok::<_, IcechunkFormatError>(item)
            })
            .try_collect()?;

        let metadata = builder.create_vector(metadata_items.as_slice());

        let enabled_feature_flags =
            sorted_enabled_feature_flags.map(|it| builder.create_vector_from_iter(it));
        let disabled_feature_flags =
            sorted_disabled_feature_flags.map(|it| builder.create_vector_from_iter(it));

        let (latest_updates, repo_before_updates) = Self::mk_latest_updates(
            &mut builder,
            update,
            backup_path,
            num_updates_per_file,
            previous_info,
        )?;

        let config = config_bytes.map(|bytes| builder.create_vector(bytes));

        // TODO: provide accessors for last_updated_at, status, metadata, etc.
        let repo_args = generated::RepoArgs {
            tags: Some(tags),
            branches: Some(branches),
            deleted_tags: Some(deleted_tags),
            snapshots: Some(snapshots),
            spec_version: spec_version as u8,
            status: Some(status),
            metadata: Some(metadata),
            latest_updates: Some(latest_updates),
            repo_before_updates,
            config,
            enabled_feature_flags,
            disabled_feature_flags,
            ..Default::default()
        };
        let repo = generated::Repo::create(&mut builder, &repo_args);
        builder.finish(repo, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Ok(Self { buffer })
    }

    #[expect(clippy::type_complexity)]
    fn mk_latest_updates<
        'bldr,
        'a,
        I: IntoIterator<Item = IcechunkResult<(UpdateType, DateTime<Utc>, Option<&'a str>)>>,
    >(
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
        update: UpdateInfo<I>,
        backup_path: Option<&'a str>,
        num_updates_per_file: u16,
        previous_info: Option<&'a str>,
    ) -> IcechunkResult<(
        WIPOffset<
            flatbuffers::Vector<
                'bldr,
                flatbuffers::ForwardsUOffset<generated::Update<'bldr>>,
            >,
        >,
        Option<WIPOffset<&'bldr str>>,
    )> {
        // replace the backup path in the last update, that must be None, by the new backup path
        let mut previous_updates = update.previous_updates.into_iter();
        let last_update = previous_updates.next().map(|maybe_data| {
            maybe_data.map(|(ut, dt, path)| {
                assert!(
                    path.is_none(),
                    "Invalid latest update iterator, last element has backup path"
                );
                (ut, dt, backup_path)
            })
        });
        // A backup_path points to the previous repo info file. It is only meaningful
        // when there are previous updates (you can't back up what doesn't exist).
        // However, previous updates CAN exist without a backup_path: during migration,
        // synthetic ops log entries are generated with no prior repo info file to
        // reference. (This differs from RepoInitializedUpdate, which has neither
        // previous updates nor a backup path.)
        assert!(
            backup_path.is_none() || last_update.is_some(),
            "A backup path must not be provided without previous updates"
        );

        // Reject updates whose timestamp is not strictly newer than the top of the ops log
        if let Some(Ok((_, latest_time, _))) = &last_update
            && update.update_time <= *latest_time
        {
            return Err(IcechunkFormatErrorKind::InvalidUpdateTimestamp {
                latest_time: *latest_time,
                new_time: update.update_time,
            })
            .capture();
        }

        let new_updates: Box<dyn Iterator<Item = _>> =
            if let Some(last_update) = last_update {
                Box::new(
                    [Ok((update.update_type, update.update_time, None)), last_update]
                        .into_iter(),
                )
            } else {
                Box::new([Ok((update.update_type, update.update_time, None))].into_iter())
            };

        let all_updates = new_updates.into_iter().chain(previous_updates);
        // If we didn't overflow (all previous updates fit in the new file),
        // preserve the old file's chain pointer so older history remains reachable.
        let mut repo_before_updates = previous_info;

        let num_updates = num_updates_per_file as usize;
        let mut updates = Vec::new();
        for maybe_data in all_updates {
            let (u_type, u_time, file) = maybe_data?;

            // Once we've reached the target file size, look for a valid overflow
            // point: an entry whose backup_path we can use as the chain pointer.
            // Entries without a backup_path (e.g. synthetic migration entries)
            // can't serve as overflow — keep them in the file instead.
            if updates.len() >= num_updates
                && let Some(bp) = file
            {
                repo_before_updates = Some(bp);
                break;
            }

            let (update_type_type, update_type) = update_type_to_fb(builder, &u_type)?;
            let file = file.map(|file| builder.create_string(file));
            updates.push(generated::Update::create(
                builder,
                &generated::UpdateArgs {
                    update_type_type,
                    update_type: Some(update_type),
                    updated_at: u_time.timestamp_micros() as u64,
                    backup_path: file,
                },
            ));
        }

        debug_assert!(!updates.is_empty(), "Must have at least one update in repo file");

        let updates = builder.create_vector(&updates);
        let repo_before_updates = repo_before_updates.map(|s| builder.create_string(s));
        Ok((updates, repo_before_updates))
    }

    pub fn initial(
        spec_version: SpecVersionBin,
        snapshot: SnapshotInfo,
        num_updates_per_file: u16,
        config: Option<&RepositoryConfig>,
        update_time: Option<DateTime<Utc>>,
    ) -> Self {
        #[expect(clippy::expect_used)]
        let config_bytes =
            config.map(|c| flexbuffers::to_vec(c).expect("Cannot serialize config"));
        // This method is basically constant, so it's OK to unwrap in it
        #[expect(clippy::expect_used)]
        Self::from_parts(
            spec_version,
            [],
            [("main", 0)],
            [],
            [snapshot],
            &Default::default(),
            UpdateInfo {
                update_type: UpdateType::RepoInitializedUpdate,
                update_time: update_time.unwrap_or_else(Utc::now),
                previous_updates: [],
            },
            None,
            num_updates_per_file,
            None,
            config_bytes.as_deref(),
            None::<std::iter::Empty<u16>>,
            None::<std::iter::Empty<u16>>,
            &RepoStatus {
                availability: RepoAvailability::Online,
                set_at: update_time.unwrap_or_else(Utc::now),
                limited_availability_reason: None,
            },
        )
        .expect("Cannot generate initial snapshot")
    }

    /// Read the raw config bytes from the `FlatBuffer` (for pass-through in mutations).
    pub(crate) fn config_bytes_raw(&self) -> IcechunkResult<Option<Vec<u8>>> {
        Ok(self.root()?.config().map(|v| v.bytes().to_vec()))
    }

    /// Read the repository configuration from the repo info.
    /// Returns `None` for repos created before config was embedded,
    /// or for repos using the default configuration.
    pub fn config(&self) -> IcechunkResult<Option<RepositoryConfig>> {
        match self.root()?.config() {
            None => Ok(None),
            Some(config_fb) => {
                let config: RepositoryConfig = flexbuffers::from_slice(config_fb.bytes())
                    .map_err(Box::new)
                    .capture()?;
                Ok(Some(config))
            }
        }
    }

    pub fn metadata(&self) -> IcechunkResult<SnapshotProperties> {
        self.root()?
            .metadata()
            .unwrap_or_default()
            .iter()
            .map(|item| {
                let key = item.name().to_string();
                let value = flexbuffers::from_slice(item.value().bytes())
                    .map_err(Box::new)
                    .capture()?;
                Ok((key, value))
            })
            .try_collect()
    }

    pub fn status(&self) -> IcechunkResult<RepoStatus> {
        let root = self.root()?;
        let fb_status = root.status();
        fb_status.try_into()
    }

    pub fn enabled_feature_flags(
        &self,
    ) -> IcechunkResult<Option<impl DoubleEndedIterator<Item = u16> + ExactSizeIterator>>
    {
        Ok(self.root()?.enabled_feature_flags().map(|v| v.iter()))
    }

    pub fn disabled_feature_flags(
        &self,
    ) -> IcechunkResult<Option<impl DoubleEndedIterator<Item = u16> + ExactSizeIterator>>
    {
        Ok(self.root()?.disabled_feature_flags().map(|v| v.iter()))
    }

    /// None means not set, use the default value
    /// Some(true) means enabled
    /// Some(false) means disabled
    pub fn feature_flag_enabled(&self, id: u16) -> IcechunkResult<Option<bool>> {
        let root = self.root()?;
        if root
            .enabled_feature_flags()
            .and_then(|v| v.lookup_by_key(id, |this, key| this.cmp(key)))
            .is_some()
        {
            return Ok(Some(true));
        }
        if root
            .disabled_feature_flags()
            .and_then(|v| v.lookup_by_key(id, |this, key| this.cmp(key)))
            .is_some()
        {
            return Ok(Some(false));
        }
        Ok(None)
    }

    fn all_tags(&self) -> IcechunkResult<impl Iterator<Item = (&str, u32)>> {
        Ok(self.root()?.tags().iter().map(|r| (r.name(), r.snapshot_index())))
    }

    fn all_branches(&self) -> IcechunkResult<impl Iterator<Item = (&str, u32)>> {
        Ok(self.root()?.branches().iter().map(|r| (r.name(), r.snapshot_index())))
    }

    pub fn deleted_tags(&self) -> IcechunkResult<impl Iterator<Item = &str>> {
        Ok(self.root()?.deleted_tags().iter())
    }

    pub fn all_snapshots(
        &self,
    ) -> IcechunkResult<impl Iterator<Item = IcechunkResult<SnapshotInfo>>> {
        let root = self.root()?;
        Ok(root.snapshots().iter().map(move |snap| mk_snapshot_info(&root, &snap)))
    }

    /// Doesn't check the validity of `flag_id`
    pub fn update_feature_flag(
        &self,
        spec_version: SpecVersionBin,
        flag_id: u16,
        enabled: Option<bool>,
        previous_file: &str,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
        let (eff, dff): (Option<Vec<_>>, Option<Vec<_>>) = match enabled {
            Some(false) => {
                let e = self
                    .enabled_feature_flags()?
                    .map(|it| it.filter(|x| *x != flag_id).collect());

                let mut d: BTreeSet<_> = self
                    .disabled_feature_flags()?
                    .map(|it| it.collect())
                    .unwrap_or_default();
                d.insert(flag_id);
                let d = d.into_iter().collect();
                (e, Some(d))
            }
            Some(true) => {
                let d = self
                    .disabled_feature_flags()?
                    .map(|it| it.filter(|x| *x != flag_id).collect());

                let mut e: BTreeSet<_> = self
                    .enabled_feature_flags()?
                    .map(|it| it.collect())
                    .unwrap_or_default();
                e.insert(flag_id);
                let e = e.into_iter().collect();
                (Some(e), d)
            }
            None => {
                let e = self
                    .enabled_feature_flags()?
                    .map(|it| it.filter(|x| *x != flag_id).collect());
                let d = self
                    .disabled_feature_flags()?
                    .map(|it| it.filter(|x| *x != flag_id).collect());
                (e, d)
            }
        };

        Self::from_parts(
            spec_version,
            self.all_tags()?,
            self.all_branches()?,
            self.deleted_tags()?,
            snaps,
            &self.metadata()?,
            UpdateInfo {
                update_type: UpdateType::FeatureFlagChanged {
                    id: flag_id,
                    new_value: enabled,
                },
                update_time: Utc::now(),
                previous_updates: self.latest_updates()?,
            },
            Some(previous_file),
            num_updates_per_file,
            self.repo_before_updates()?,
            self.config_bytes_raw()?.as_deref(),
            eff.map(|it| it.into_iter()),
            dff.map(|it| it.into_iter()),
            &self.status()?,
        )
    }

    #[expect(clippy::too_many_arguments)]
    pub fn add_snapshot(
        &self,
        spec_version: SpecVersionBin,
        snap: SnapshotInfo,
        branch: Option<&str>,
        update_type: UpdateType,
        update_time: Option<DateTime<Utc>>, // for testing
        previous_file: &str,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        let mut snapshots: Vec<_> = self.all_snapshots()?.try_collect()?;
        let new_index = match snapshots.binary_search_by_key(&&snap.id, |snap| &snap.id) {
            Ok(_) => Err(IcechunkFormatErrorKind::DuplicateSnapshotId {
                snapshot_id: snap.id.clone(),
            })
            .capture(),
            Err(idx) => Ok(idx),
        }?;

        snapshots.insert(new_index, snap);

        let tags = self.all_tags()?.map(|(name, idx)| {
            if idx as usize >= new_index { (name, idx + 1) } else { (name, idx) }
        });
        let branches = self.all_branches()?.map(|(name, idx)| {
            if Some(name) == branch {
                (name, new_index as u32)
            } else if idx as usize >= new_index {
                (name, idx + 1)
            } else {
                (name, idx)
            }
        });

        let res = Self::from_parts(
            spec_version,
            tags,
            branches,
            self.deleted_tags()?,
            snapshots,
            &self.metadata()?,
            UpdateInfo {
                update_type,
                update_time: update_time.unwrap_or_else(Utc::now),
                previous_updates: self.latest_updates()?,
            },
            Some(previous_file),
            num_updates_per_file,
            self.repo_before_updates()?,
            self.config_bytes_raw()?.as_deref(),
            self.enabled_feature_flags()?,
            self.disabled_feature_flags()?,
            &self.status()?,
        )?;
        Ok(res)
    }

    pub fn add_branch(
        &self,
        spec_version: SpecVersionBin,
        name: &str,
        snap: &SnapshotId,
        previous_file: &str,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        if let Ok(snapshot_id) = self.resolve_branch(name) {
            return Err(IcechunkFormatErrorKind::BranchAlreadyExists {
                branch: name.to_string(),
                snapshot_id,
            })
            .capture();
        }

        match self.resolve_snapshot_index(snap)? {
            Some(snap_idx) => {
                let mut branches: Vec<_> = self.all_branches()?.collect();
                branches.push((name, snap_idx as u32));
                branches.sort_by(|(name1, _), (name2, _)| name1.cmp(name2));
                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                Ok(Self::from_parts(
                    spec_version,
                    self.all_tags()?,
                    branches,
                    self.deleted_tags()?,
                    snaps,
                    &self.metadata()?,
                    UpdateInfo {
                        update_type: UpdateType::BranchCreatedUpdate {
                            name: name.to_string(),
                        },
                        update_time: Utc::now(),
                        previous_updates: self.latest_updates()?,
                    },
                    Some(previous_file),
                    num_updates_per_file,
                    self.repo_before_updates()?,
                    self.config_bytes_raw()?.as_deref(),
                    self.enabled_feature_flags()?,
                    self.disabled_feature_flags()?,
                    &self.status()?,
                )?)
            }
            None => Err(IcechunkFormatErrorKind::SnapshotIdNotFound {
                snapshot_id: snap.clone(),
            })
            .capture(),
        }
    }

    pub fn delete_branch(
        &self,
        spec_version: SpecVersionBin,
        name: &str,
        previous_file: &str,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        match self.resolve_branch(name) {
            Ok(previous_snap_id) => {
                let mut branches: Vec<_> = self.all_branches()?.collect();
                // retain preserves order
                branches.retain(|(n, _)| n != &name);
                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                Self::from_parts(
                    spec_version,
                    self.all_tags()?,
                    branches,
                    self.deleted_tags()?,
                    snaps,
                    &self.metadata()?,
                    UpdateInfo {
                        update_type: UpdateType::BranchDeletedUpdate {
                            name: name.to_string(),
                            previous_snap_id,
                        },
                        update_time: Utc::now(),
                        previous_updates: self.latest_updates()?,
                    },
                    Some(previous_file),
                    num_updates_per_file,
                    self.repo_before_updates()?,
                    self.config_bytes_raw()?.as_deref(),
                    self.enabled_feature_flags()?,
                    self.disabled_feature_flags()?,
                    &self.status()?,
                )
            }
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::BranchNotFound { .. },
                ..
            }) => {
                Err(IcechunkFormatErrorKind::BranchNotFound { branch: name.to_string() })
                    .capture()
            }
            Err(err) => Err(err),
        }
    }

    pub fn update_branch(
        &self,
        spec_version: SpecVersionBin,
        name: &str,
        new_snap: &SnapshotId,
        previous_file: &str,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        let previous_snap_id = self.resolve_branch(name)?;
        match self.resolve_snapshot_index(new_snap)? {
            Some(snap_idx) => {
                let branches = self.all_branches()?.map(|(br, idx)| {
                    if br == name { (br, snap_idx as u32) } else { (br, idx) }
                });
                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                Ok(Self::from_parts(
                    spec_version,
                    self.all_tags()?,
                    branches,
                    self.deleted_tags()?,
                    snaps,
                    &self.metadata()?,
                    UpdateInfo {
                        update_type: UpdateType::BranchResetUpdate {
                            name: name.to_string(),
                            previous_snap_id,
                        },
                        update_time: Utc::now(),
                        previous_updates: self.latest_updates()?,
                    },
                    Some(previous_file),
                    num_updates_per_file,
                    self.repo_before_updates()?,
                    self.config_bytes_raw()?.as_deref(),
                    self.enabled_feature_flags()?,
                    self.disabled_feature_flags()?,
                    &self.status()?,
                )?)
            }
            None => Err(IcechunkFormatErrorKind::SnapshotIdNotFound {
                snapshot_id: new_snap.clone(),
            })
            .capture(),
        }
    }

    pub fn add_tag(
        &self,
        spec_version: SpecVersionBin,
        name: &str,
        snap: &SnapshotId,
        previous_file: &str,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        if self.resolve_tag(name).is_ok() {
            return Err(IcechunkFormatErrorKind::TagAlreadyExists {
                tag: name.to_string(),
            })
            .capture();
        }
        if self.tag_was_deleted(name)? {
            return Err(IcechunkFormatErrorKind::TagPreviouslyDeleted {
                tag: name.to_string(),
            })
            .capture();
        }

        match self.resolve_snapshot_index(snap)? {
            Some(snap_idx) => {
                let mut tags: Vec<_> = self.all_tags()?.collect();
                tags.push((name, snap_idx as u32));
                tags.sort_by(|(name1, _), (name2, _)| name1.cmp(name2));
                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                Ok(Self::from_parts(
                    spec_version,
                    tags,
                    self.all_branches()?,
                    self.deleted_tags()?,
                    snaps,
                    &self.metadata()?,
                    UpdateInfo {
                        update_type: UpdateType::TagCreatedUpdate {
                            name: name.to_string(),
                        },
                        update_time: Utc::now(),
                        previous_updates: self.latest_updates()?,
                    },
                    Some(previous_file),
                    num_updates_per_file,
                    self.repo_before_updates()?,
                    self.config_bytes_raw()?.as_deref(),
                    self.enabled_feature_flags()?,
                    self.disabled_feature_flags()?,
                    &self.status()?,
                )?)
            }
            None => Err(IcechunkFormatErrorKind::SnapshotIdNotFound {
                snapshot_id: snap.clone(),
            })
            .capture(),
        }
    }

    pub fn delete_tag(
        &self,
        spec_version: SpecVersionBin,
        name: &str,
        previous_file: &str,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        match self.resolve_tag(name) {
            Ok(previous_snap_id) => {
                let mut tags: Vec<_> = self.all_tags()?.collect();
                // retain preserves order
                tags.retain(|(n, _)| n != &name);

                let mut deleted_tags: BTreeSet<_> = self.deleted_tags()?.collect();
                debug_assert!(!deleted_tags.contains(name));
                deleted_tags.insert(name);

                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                Self::from_parts(
                    spec_version,
                    tags,
                    self.all_branches()?,
                    deleted_tags,
                    snaps,
                    &self.metadata()?,
                    UpdateInfo {
                        update_type: UpdateType::TagDeletedUpdate {
                            name: name.to_string(),
                            previous_snap_id,
                        },
                        update_time: Utc::now(),
                        previous_updates: self.latest_updates()?,
                    },
                    Some(previous_file),
                    num_updates_per_file,
                    self.repo_before_updates()?,
                    self.config_bytes_raw()?.as_deref(),
                    self.enabled_feature_flags()?,
                    self.disabled_feature_flags()?,
                    &self.status()?,
                )
            }
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::TagNotFound { .. },
                ..
            }) => Err(IcechunkFormatErrorKind::TagNotFound { tag: name.to_string() })
                .capture(),
            Err(err) => Err(err),
        }
    }

    pub fn set_metadata(
        &self,
        spec_version: SpecVersionBin,
        metadata: &SnapshotProperties,
        previous_file: &str,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
        Self::from_parts(
            spec_version,
            self.all_tags()?,
            self.all_branches()?,
            self.deleted_tags()?,
            snaps,
            metadata,
            UpdateInfo {
                update_type: UpdateType::MetadataChangedUpdate,
                update_time: Utc::now(),
                previous_updates: self.latest_updates()?,
            },
            Some(previous_file),
            num_updates_per_file,
            self.repo_before_updates()?,
            self.config_bytes_raw()?.as_deref(),
            self.enabled_feature_flags()?,
            self.disabled_feature_flags()?,
            &self.status()?,
        )
    }

    /// Update the embedded configuration and record a `ConfigChangedUpdate` in the op log.
    pub fn set_config(
        &self,
        spec_version: SpecVersionBin,
        config: &RepositoryConfig,
        previous_file: &str,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        let config_bytes = flexbuffers::to_vec(config).map_err(Box::new).capture()?;
        let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
        Self::from_parts(
            spec_version,
            self.all_tags()?,
            self.all_branches()?,
            self.deleted_tags()?,
            snaps,
            &self.metadata()?,
            UpdateInfo {
                update_type: UpdateType::ConfigChangedUpdate,
                update_time: Utc::now(),
                previous_updates: self.latest_updates()?,
            },
            Some(previous_file),
            num_updates_per_file,
            self.repo_before_updates()?,
            Some(config_bytes.as_slice()),
            self.enabled_feature_flags()?,
            self.disabled_feature_flags()?,
            &self.status()?,
        )
    }

    pub fn set_status(
        &self,
        spec_version: SpecVersionBin,
        status: &RepoStatus,
        previous_file: &str,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
        Self::from_parts(
            spec_version,
            self.all_tags()?,
            self.all_branches()?,
            self.deleted_tags()?,
            snaps,
            &self.metadata()?,
            UpdateInfo {
                update_type: UpdateType::RepoStatusChangedUpdate {
                    status: status.clone(),
                },
                update_time: Utc::now(),
                previous_updates: self.latest_updates()?,
            },
            Some(previous_file),
            num_updates_per_file,
            self.repo_before_updates()?,
            self.config_bytes_raw()?.as_deref(),
            self.enabled_feature_flags()?,
            self.disabled_feature_flags()?,
            status,
        )
    }

    pub fn from_buffer(buffer: Vec<u8>) -> IcechunkResult<RepoInfo> {
        let _ = flatbuffers::root_with_opts::<generated::Repo<'_>>(
            &ROOT_OPTIONS,
            buffer.as_slice(),
        )
        .capture()?;
        Ok(RepoInfo { buffer })
    }

    pub fn bytes(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    fn root(&self) -> IcechunkResult<generated::Repo<'_>> {
        flatbuffers::root::<generated::Repo<'_>>(&self.buffer).capture()
    }

    pub fn tag_names(&self) -> IcechunkResult<impl Iterator<Item = &str>> {
        Ok(self.root()?.tags().iter().map(|r| r.name()))
    }

    pub fn branch_names(&self) -> IcechunkResult<impl Iterator<Item = &str>> {
        Ok(self.root()?.branches().iter().map(|r| r.name()))
    }

    pub fn tags(&self) -> IcechunkResult<impl Iterator<Item = (&str, SnapshotId)>> {
        let root = self.root()?;
        Ok(self.all_tags()?.map(move |(name, idx)| {
            (name, SnapshotId::new(root.snapshots().get(idx as usize).id().0))
        }))
    }

    pub fn branches(&self) -> IcechunkResult<impl Iterator<Item = (&str, SnapshotId)>> {
        let root = self.root()?;
        Ok(self.all_branches()?.map(move |(name, idx)| {
            (name, SnapshotId::new(root.snapshots().get(idx as usize).id().0))
        }))
    }

    pub fn resolve_tag(&self, name: &str) -> IcechunkResult<SnapshotId> {
        let root = self.root()?;
        let res = root
            .tags()
            .lookup_by_key(name, |r, key| r.name().cmp(key))
            .map(|r| {
                let index = r.snapshot_index();
                SnapshotId::new(root.snapshots().get(index as usize).id().0)
            })
            .ok_or_else(|| IcechunkFormatErrorKind::TagNotFound { tag: name.to_string() })
            .capture()?;

        Ok(res)
    }

    pub fn tag_was_deleted(&self, name: &str) -> IcechunkResult<bool> {
        let root = self.root()?;
        let res = root.deleted_tags().lookup_by_key(name, |name, key| name.cmp(key));
        Ok(res.is_some())
    }

    pub fn resolve_branch(&self, name: &str) -> IcechunkResult<SnapshotId> {
        let root = self.root()?;
        let res = root
            .branches()
            .lookup_by_key(name, |r, key| r.name().cmp(key))
            .map(|r| {
                let index = r.snapshot_index();
                SnapshotId::new(root.snapshots().get(index as usize).id().0)
            })
            .ok_or_else(|| IcechunkFormatErrorKind::BranchNotFound {
                branch: name.to_string(),
            })
            .capture()?;

        Ok(res)
    }

    pub fn spec_version(&self) -> IcechunkResult<SpecVersionBin> {
        let raw = self.root()?.spec_version();
        raw.try_into()
            .map_err(|_| IcechunkFormatErrorKind::InvalidSpecVersion {
                found: raw,
                max_supported: SpecVersionBin::current() as u8,
            })
            .capture()
    }

    pub fn latest_updates(
        &self,
    ) -> IcechunkResult<impl Iterator<Item = UpdateTuple<'_>>> {
        let res = self.root()?.latest_updates().iter().map(|up| self.update_to_tuple(up));
        Ok(res)
    }

    fn update_to_tuple<'a>(
        &'a self,
        update: generated::Update<'a>,
    ) -> IcechunkResult<(UpdateType, DateTime<Utc>, Option<&'a str>)> {
        let ty = self.mk_update_type(&update)?;
        let ts = timestamp_to_timestamp(update.updated_at())?;
        let bp = update.backup_path();
        Ok((ty, ts, bp))
    }

    fn mk_update_type(
        &self,
        update: &generated::Update<'_>,
    ) -> IcechunkResult<UpdateType> {
        #[expect(clippy::unwrap_used)]
        match update.update_type_type() {
            generated::UpdateType::RepoInitializedUpdate => {
                Ok(UpdateType::RepoInitializedUpdate)
            }
            generated::UpdateType::RepoMigratedUpdate => {
                let up = update.update_type_as_repo_migrated_update().unwrap();
                let from_raw = up.from_version();
                let to_raw = up.to_version();
                Ok(UpdateType::RepoMigratedUpdate {
                    from_version: from_raw
                        .try_into()
                        .map_err(|_| IcechunkFormatErrorKind::InvalidSpecVersion {
                            found: from_raw,
                            max_supported: SpecVersionBin::current() as u8,
                        })
                        .capture()?,
                    to_version: to_raw
                        .try_into()
                        .map_err(|_| IcechunkFormatErrorKind::InvalidSpecVersion {
                            found: to_raw,
                            max_supported: SpecVersionBin::current() as u8,
                        })
                        .capture()?,
                })
            }
            generated::UpdateType::RepoStatusChangedUpdate => {
                let up = update.update_type_as_repo_status_changed_update().unwrap();
                let fb_status = up.status().unwrap();
                let status = fb_status.try_into()?;

                Ok(UpdateType::RepoStatusChangedUpdate { status })
            }
            generated::UpdateType::ConfigChangedUpdate => {
                Ok(UpdateType::ConfigChangedUpdate)
            }
            generated::UpdateType::MetadataChangedUpdate => {
                Ok(UpdateType::MetadataChangedUpdate)
            }
            generated::UpdateType::TagCreatedUpdate => {
                let up = update.update_type_as_tag_created_update().unwrap();
                Ok(UpdateType::TagCreatedUpdate { name: up.name().to_string() })
            }
            generated::UpdateType::TagDeletedUpdate => {
                let up = update.update_type_as_tag_deleted_update().unwrap();
                let previous_snap_id = SnapshotId::new(up.previous_snap_id().0);
                Ok(UpdateType::TagDeletedUpdate {
                    name: up.name().to_string(),
                    previous_snap_id,
                })
            }
            generated::UpdateType::BranchCreatedUpdate => {
                let up = update.update_type_as_branch_created_update().unwrap();
                Ok(UpdateType::BranchCreatedUpdate { name: up.name().to_string() })
            }
            generated::UpdateType::BranchDeletedUpdate => {
                let up = update.update_type_as_branch_deleted_update().unwrap();
                let previous_snap_id = SnapshotId::new(up.previous_snap_id().0);
                Ok(UpdateType::BranchDeletedUpdate {
                    name: up.name().to_string(),
                    previous_snap_id,
                })
            }
            generated::UpdateType::BranchResetUpdate => {
                let up = update.update_type_as_branch_reset_update().unwrap();
                let previous_snap_id = SnapshotId::new(up.previous_snap_id().0);
                Ok(UpdateType::BranchResetUpdate {
                    name: up.name().to_string(),
                    previous_snap_id,
                })
            }
            generated::UpdateType::NewCommitUpdate => {
                let up = update.update_type_as_new_commit_update().unwrap();
                let new_snap_id = SnapshotId::new(up.new_snap_id().0);
                Ok(UpdateType::NewCommitUpdate {
                    branch: up.branch().to_string(),
                    new_snap_id,
                })
            }
            generated::UpdateType::CommitAmendedUpdate => {
                let up = update.update_type_as_commit_amended_update().unwrap();
                let previous_snap_id = SnapshotId::new(up.previous_snap_id().0);
                let new_snap_id = SnapshotId::new(up.new_snap_id().0);
                Ok(UpdateType::CommitAmendedUpdate {
                    branch: up.branch().to_string(),
                    previous_snap_id,
                    new_snap_id,
                })
            }
            generated::UpdateType::NewDetachedSnapshotUpdate => {
                let up = update.update_type_as_new_detached_snapshot_update().unwrap();
                let new_snap_id = SnapshotId::new(up.new_snap_id().0);
                Ok(UpdateType::NewDetachedSnapshotUpdate { new_snap_id })
            }
            generated::UpdateType::GCRanUpdate => Ok(UpdateType::GCRanUpdate),
            generated::UpdateType::ExpirationRanUpdate => {
                Ok(UpdateType::ExpirationRanUpdate)
            }
            generated::UpdateType::FeatureFlagChangedUpdate => {
                let up = update.update_type_as_feature_flag_changed_update().unwrap();
                Ok(UpdateType::FeatureFlagChanged {
                    id: up.id(),
                    new_value: if up.is_set() { Some(up.new_value()) } else { None },
                })
            }
            _ => Err(IcechunkFormatErrorKind::InvalidFlatBuffer(
                flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                    field: Cow::Borrowed("latest_update_type"),
                    field_type: Cow::Borrowed("UpdateType"),
                    error_trace: Default::default(),
                },
            ))
            .capture(),
        }
    }

    pub fn ancestry<'a>(
        &'a self,
        snapshot: &SnapshotId,
    ) -> IcechunkResult<impl Iterator<Item = IcechunkResult<SnapshotInfo>> + Send + use<'a>>
    {
        let root = self.root()?;
        if let Some(start) = self.resolve_snapshot_index(snapshot)? {
            let mut index = Some(start as i32);
            let iter = std::iter::from_fn(move || {
                if let Some(ix) = index {
                    if ix >= 0 {
                        let snap = root.snapshots().get(ix as usize);
                        index = Some(snap.parent_offset());
                        Some(mk_snapshot_info(&root, &snap))
                    } else {
                        index = None;
                        None
                    }
                } else {
                    None
                }
            });
            Ok(iter)
        } else {
            Err(IcechunkFormatErrorKind::SnapshotIdNotFound {
                snapshot_id: snapshot.clone(),
            })
            .capture()
        }
    }

    pub fn find_snapshot(&self, id: &SnapshotId) -> IcechunkResult<SnapshotInfo> {
        let mut anc = self.ancestry(id)?;
        #[expect(clippy::panic)]
        match anc.next() {
            Some(snap) => snap,
            // It's OK to panic here because ancestry already found the snapshot, and
            // it's always the first element of the ancestry
            None => panic!("Ancestry head snapshot not found"),
        }
    }

    pub fn repo_before_updates(&self) -> IcechunkResult<Option<&str>> {
        Ok(self.root()?.repo_before_updates())
    }

    fn resolve_snapshot_index(&self, id: &SnapshotId) -> IcechunkResult<Option<usize>> {
        Ok(lookup_index_by_key(self.root()?.snapshots(), &id.0, |snap, key| {
            snap.id().0.cmp(key)
        }))
    }
}

fn resolve_ref_iter<'a>(
    sorted_snapshots: &[SnapshotInfo],
    it: impl IntoIterator<Item = (&'a str, SnapshotId)>,
) -> IcechunkResult<Vec<(&'a str, u32)>> {
    let mut res: Vec<_> = it
        .into_iter()
        .map(|(name, id)| {
            let idx = sorted_snapshots
                .binary_search_by_key(&&id.0, |snap| &snap.id.0)
                .map_err(|_| IcechunkFormatErrorKind::SnapshotIdNotFound {
                    snapshot_id: id.clone(),
                })
                .capture()? as u32;
            Ok::<_, IcechunkFormatError>((name, idx))
        })
        .try_collect()?;
    res.sort_by(|(name1, _), (name2, _)| name1.cmp(name2));
    Ok(res)
}

fn timestamp_to_timestamp(ts: u64) -> IcechunkResult<DateTime<Utc>> {
    let ts: i64 =
        ts.try_into().map_err(|_| IcechunkFormatErrorKind::InvalidTimestamp).capture()?;
    DateTime::from_timestamp_micros(ts)
        .ok_or(IcechunkFormatErrorKind::InvalidTimestamp)
        .capture()
}

fn mk_snapshot_info(
    repo: &generated::Repo<'_>,
    snap: &generated::SnapshotInfo<'_>,
) -> IcechunkResult<SnapshotInfo> {
    let flushed_at = timestamp_to_timestamp(snap.flushed_at())?;
    let parent_id = if snap.parent_offset() >= 0 {
        let parent = repo.snapshots().get(snap.parent_offset() as usize).id();
        Some(parent)
    } else {
        None
    };
    let metadata = snap
        .metadata()
        .map(|items| {
            let items = items
                .iter()
                .map(|item| {
                    let name = item.name().to_string();
                    let value = flexbuffers::from_slice(item.value().bytes())
                        .map_err(Box::new)
                        .capture()?;
                    Ok::<_, IcechunkFormatError>((name, value))
                })
                .try_collect()?;
            Ok::<_, IcechunkFormatError>(items)
        })
        .transpose()?
        .unwrap_or_default();

    Ok(SnapshotInfo {
        id: SnapshotId::new(snap.id().0),
        flushed_at,
        message: snap.message().to_string(),
        metadata,
        parent_id: parent_id.map(|buf| SnapshotId::new(buf.0)),
    })
}

fn update_type_to_fb<'bldr>(
    builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    update: &UpdateType,
) -> IcechunkResult<(generated::UpdateType, WIPOffset<UnionWIPOffset>)> {
    match update {
        UpdateType::RepoInitializedUpdate => Ok((
            generated::UpdateType::RepoInitializedUpdate,
            generated::RepoInitializedUpdate::create(
                builder,
                &generated::RepoInitializedUpdateArgs {},
            )
            .as_union_value(),
        )),
        UpdateType::RepoMigratedUpdate { from_version, to_version } => Ok((
            generated::UpdateType::RepoMigratedUpdate,
            generated::RepoMigratedUpdate::create(
                builder,
                &generated::RepoMigratedUpdateArgs {
                    from_version: *from_version as u8,
                    to_version: *to_version as u8,
                },
            )
            .as_union_value(),
        )),
        UpdateType::RepoStatusChangedUpdate { status } => {
            let limited_availability_reason = status
                .limited_availability_reason
                .as_ref()
                .map(|r| builder.create_string(r));
            let status = generated::RepoStatus::create(
                builder,
                &generated::RepoStatusArgs {
                    availability: status.availability.into(),
                    set_at: status.set_at.timestamp_micros() as u64,
                    limited_availability_reason,
                },
            );

            Ok((
                generated::UpdateType::RepoStatusChangedUpdate,
                generated::RepoStatusChangedUpdate::create(
                    builder,
                    &generated::RepoStatusChangedUpdateArgs { status: Some(status) },
                )
                .as_union_value(),
            ))
        }
        UpdateType::ConfigChangedUpdate => Ok((
            generated::UpdateType::ConfigChangedUpdate,
            generated::ConfigChangedUpdate::create(
                builder,
                &generated::ConfigChangedUpdateArgs {},
            )
            .as_union_value(),
        )),
        UpdateType::MetadataChangedUpdate => Ok((
            generated::UpdateType::MetadataChangedUpdate,
            generated::MetadataChangedUpdate::create(
                builder,
                &generated::MetadataChangedUpdateArgs {},
            )
            .as_union_value(),
        )),
        UpdateType::TagCreatedUpdate { name } => {
            let name = Some(builder.create_string(name));
            Ok((
                generated::UpdateType::TagCreatedUpdate,
                generated::TagCreatedUpdate::create(
                    builder,
                    &generated::TagCreatedUpdateArgs { name },
                )
                .as_union_value(),
            ))
        }
        UpdateType::TagDeletedUpdate { name, previous_snap_id } => {
            let name = Some(builder.create_string(name));
            let object_id12 = generated::ObjectId12::new(&previous_snap_id.0);
            let previous_snap_id = Some(&object_id12);
            Ok((
                generated::UpdateType::TagDeletedUpdate,
                generated::TagDeletedUpdate::create(
                    builder,
                    &generated::TagDeletedUpdateArgs { name, previous_snap_id },
                )
                .as_union_value(),
            ))
        }
        UpdateType::BranchCreatedUpdate { name } => {
            let name = Some(builder.create_string(name));
            Ok((
                generated::UpdateType::BranchCreatedUpdate,
                generated::BranchCreatedUpdate::create(
                    builder,
                    &generated::BranchCreatedUpdateArgs { name },
                )
                .as_union_value(),
            ))
        }
        UpdateType::BranchDeletedUpdate { name, previous_snap_id } => {
            let name = Some(builder.create_string(name));
            let object_id12 = generated::ObjectId12::new(&previous_snap_id.0);
            let previous_snap_id = Some(&object_id12);
            Ok((
                generated::UpdateType::BranchDeletedUpdate,
                generated::BranchDeletedUpdate::create(
                    builder,
                    &generated::BranchDeletedUpdateArgs { name, previous_snap_id },
                )
                .as_union_value(),
            ))
        }
        UpdateType::BranchResetUpdate { name, previous_snap_id } => {
            let name = Some(builder.create_string(name));
            let object_id12 = generated::ObjectId12::new(&previous_snap_id.0);
            let previous_snap_id = Some(&object_id12);
            Ok((
                generated::UpdateType::BranchResetUpdate,
                generated::BranchResetUpdate::create(
                    builder,
                    &generated::BranchResetUpdateArgs { name, previous_snap_id },
                )
                .as_union_value(),
            ))
        }
        UpdateType::NewCommitUpdate { branch, new_snap_id } => {
            let branch = Some(builder.create_string(branch));
            let object_id12 = generated::ObjectId12::new(&new_snap_id.0);
            let new_snap_id = Some(&object_id12);
            Ok((
                generated::UpdateType::NewCommitUpdate,
                generated::NewCommitUpdate::create(
                    builder,
                    &generated::NewCommitUpdateArgs { branch, new_snap_id },
                )
                .as_union_value(),
            ))
        }
        UpdateType::CommitAmendedUpdate { branch, previous_snap_id, new_snap_id } => {
            let branch = Some(builder.create_string(branch));
            let object_id12 = generated::ObjectId12::new(&previous_snap_id.0);
            let previous_snap_id = Some(&object_id12);
            let object_id12 = generated::ObjectId12::new(&new_snap_id.0);
            let new_snap_id = Some(&object_id12);
            Ok((
                generated::UpdateType::CommitAmendedUpdate,
                generated::CommitAmendedUpdate::create(
                    builder,
                    &generated::CommitAmendedUpdateArgs {
                        branch,
                        previous_snap_id,
                        new_snap_id,
                    },
                )
                .as_union_value(),
            ))
        }
        UpdateType::NewDetachedSnapshotUpdate { new_snap_id } => {
            let object_id12 = generated::ObjectId12::new(&new_snap_id.0);
            let new_snap_id = Some(&object_id12);
            Ok((
                generated::UpdateType::NewDetachedSnapshotUpdate,
                generated::NewDetachedSnapshotUpdate::create(
                    builder,
                    &generated::NewDetachedSnapshotUpdateArgs { new_snap_id },
                )
                .as_union_value(),
            ))
        }
        UpdateType::GCRanUpdate => Ok((
            generated::UpdateType::GCRanUpdate,
            generated::GCRanUpdate::create(builder, &generated::GCRanUpdateArgs {})
                .as_union_value(),
        )),
        UpdateType::ExpirationRanUpdate => Ok((
            generated::UpdateType::ExpirationRanUpdate,
            generated::ExpirationRanUpdate::create(
                builder,
                &generated::ExpirationRanUpdateArgs {},
            )
            .as_union_value(),
        )),
        UpdateType::FeatureFlagChanged { id, new_value } => Ok((
            generated::UpdateType::FeatureFlagChangedUpdate,
            generated::FeatureFlagChangedUpdate::create(
                builder,
                &generated::FeatureFlagChangedUpdateArgs {
                    id: *id,
                    new_value: new_value.unwrap_or_default(),
                    is_set: new_value.is_some(),
                },
            )
            .as_union_value(),
        )),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::roundtrip_serialization_tests;
    use proptest::prelude::*;
    use std::collections::HashSet;

    // Generates an instance of RepoInfo which may not deserialize to a valid repository
    fn potentially_invalid_repo_info() -> impl Strategy<Value = RepoInfo> {
        any::<Vec<u8>>().prop_map(|buffer| RepoInfo { buffer })
    }

    roundtrip_serialization_tests!(
        serialize_and_deserialize_repo_info - potentially_invalid_repo_info
    );

    #[test]
    fn test_add_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let id1 = SnapshotId::random();
        let snap1 = SnapshotInfo {
            id: id1.clone(),
            parent_id: None,
            // needs to be micro second rounded
            flushed_at: DateTime::from_timestamp_micros(1_000_000).unwrap(),
            message: "snap 1".to_string(),
            metadata: Default::default(),
        };
        let repo =
            RepoInfo::initial(SpecVersionBin::current(), snap1.clone(), 100, None, None);
        assert_eq!(repo.all_snapshots()?.next().unwrap().unwrap(), snap1);

        let id2 = SnapshotId::random();
        let snap2 = SnapshotInfo {
            id: id2.clone(),
            parent_id: Some(id1.clone()),
            flushed_at: DateTime::from_timestamp_micros(2_000_000).unwrap(),
            message: "snap 2".to_string(),
            ..snap1.clone()
        };
        let repo = repo.add_snapshot(
            SpecVersionBin::current(),
            snap2.clone(),
            Some("main"),
            UpdateType::NewCommitUpdate {
                branch: "main".to_string(),
                new_snap_id: snap2.id.clone(),
            },
            None,
            "foo/bar",
            100,
        )?;
        assert_eq!(&repo.resolve_branch("main")?, &snap2.id);
        assert_eq!(repo.repo_before_updates()?, None);

        let all: HashSet<_> = repo.all_snapshots()?.try_collect()?;
        assert_eq!(all, HashSet::from_iter([snap1.clone(), snap2.clone()]));

        let anc: Vec<_> = repo.ancestry(&id1)?.try_collect()?;
        assert_eq!(anc, std::slice::from_ref(&snap1));

        let anc: Vec<_> = repo.ancestry(&id2)?.try_collect()?;
        assert_eq!(anc, [snap2.clone(), snap1.clone()]);

        assert!(repo.ancestry(&SnapshotId::random()).is_err());

        let id3 = SnapshotId::random();
        let snap3 = SnapshotInfo {
            id: id3.clone(),
            parent_id: Some(id2.clone()),
            flushed_at: DateTime::from_timestamp_micros(3_000_000).unwrap(),
            message: "snap 3".to_string(),
            ..snap2.clone()
        };
        let repo = repo.add_snapshot(
            SpecVersionBin::current(),
            snap3.clone(),
            Some("main"),
            UpdateType::NewCommitUpdate {
                branch: "main".to_string(),
                new_snap_id: snap3.id.clone(),
            },
            None,
            "foo",
            100,
        )?;
        assert_eq!(&repo.resolve_branch("main")?, &snap3.id);
        let all: HashSet<_> = repo.all_snapshots()?.try_collect()?;
        assert_eq!(
            all,
            HashSet::from_iter([snap1.clone(), snap2.clone(), snap3.clone()])
        );

        let all: HashSet<_> = repo.all_snapshots()?.try_collect()?;
        assert_eq!(
            all,
            HashSet::from_iter([snap1.clone(), snap2.clone(), snap3.clone()])
        );

        let anc: Vec<_> = repo.ancestry(&id3)?.try_collect()?;
        assert_eq!(anc, [snap3.clone(), snap2.clone(), snap1.clone()]);
        Ok(())
    }

    #[test]
    fn test_tags_and_branches() -> Result<(), Box<dyn std::error::Error>> {
        let id1 = SnapshotId::random();
        let snap1 = SnapshotInfo {
            id: id1.clone(),
            parent_id: None,
            // needs to be micro second rounded
            flushed_at: DateTime::from_timestamp_micros(1_000_000).unwrap(),
            message: "snap 1".to_string(),
            metadata: Default::default(),
        };
        let repo =
            RepoInfo::initial(SpecVersionBin::current(), snap1.clone(), 100, None, None);
        let repo = repo.add_branch(SpecVersionBin::current(), "foo", &id1, "foo", 100)?;
        let repo = repo.add_branch(SpecVersionBin::current(), "bar", &id1, "bar", 100)?;
        assert!(matches!(
            repo.add_branch(
                SpecVersionBin::current(),
                "bad-snap",
                &SnapshotId::random(),
                "bad",
                100
            ),
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::SnapshotIdNotFound { .. },
                ..
            })
        ));
        // cannot add existing
        assert!(matches!(
            repo.add_branch(SpecVersionBin::current(), "bar", &id1, "/foo/bar", 100),
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::BranchAlreadyExists { .. },
                ..
            })
        ));

        assert_eq!(
            repo.all_branches()?.collect::<HashSet<_>>(),
            [("main", 0), ("foo", 0), ("bar", 0)].into()
        );

        let id2 = SnapshotId::random();
        let snap2 = SnapshotInfo {
            id: id2.clone(),
            parent_id: Some(id1.clone()),
            flushed_at: Utc::now(),
            message: "snap 2".to_string(),
            ..snap1.clone()
        };
        let repo = repo.add_snapshot(
            SpecVersionBin::current(),
            snap2.clone(),
            Some("main"),
            UpdateType::NewCommitUpdate {
                branch: "main".to_string(),
                new_snap_id: snap2.id.clone(),
            },
            None,
            "foo",
            100,
        )?;
        let repo =
            repo.add_branch(SpecVersionBin::current(), "baz", &id2, "/foo/bar", 100)?;
        assert_eq!(repo.resolve_branch("main")?, id2.clone());
        assert_eq!(repo.resolve_branch("foo")?, id1.clone());
        assert_eq!(repo.resolve_branch("bar")?, id1.clone());
        assert_eq!(repo.resolve_branch("baz")?, id2.clone());

        let repo = repo.delete_branch(SpecVersionBin::current(), "bar", "bar", 100)?;
        assert!(repo.resolve_branch("bar").is_err());
        assert_eq!(
            repo.all_branches()?.map(|(n, _)| n).collect::<HashSet<_>>(),
            ["main", "foo", "baz"].into()
        );

        assert!(
            repo.delete_branch(SpecVersionBin::current(), "bad-branch", "bad", 100)
                .is_err()
        );

        // tags
        let repo = repo.add_tag(SpecVersionBin::current(), "tag1", &id1, "tag1", 100)?;
        let repo = repo.add_tag(SpecVersionBin::current(), "tag2", &id2, "tag2", 100)?;
        assert!(
            repo.add_tag(
                SpecVersionBin::current(),
                "bad-snap",
                &SnapshotId::random(),
                "bad",
                100
            )
            .is_err()
        );
        assert!(
            repo.add_tag(SpecVersionBin::current(), "tag1", &id1, "tag1-again", 100)
                .is_err()
        );
        assert_eq!(repo.resolve_tag("tag1")?, id1.clone());
        assert_eq!(repo.resolve_tag("tag2")?, id2.clone());
        assert_eq!(
            repo.all_tags()?.map(|(n, _)| n).collect::<HashSet<_>>(),
            ["tag1", "tag2"].into()
        );

        // delete tags
        let repo = repo.add_tag(SpecVersionBin::current(), "tag3", &id1, "tag3", 100)?;
        let repo =
            repo.delete_tag(SpecVersionBin::current(), "tag3", "delete-tag3", 100)?;
        assert_eq!(
            repo.all_tags()?.map(|(n, _)| n).collect::<HashSet<_>>(),
            ["tag1", "tag2"].into()
        );
        // cannot add deleted
        assert!(
            repo.add_tag(SpecVersionBin::current(), "tag3", &id1, "tag3-again", 100)
                .is_err()
        );
        // cannot delete deleted
        assert!(
            repo.delete_tag(SpecVersionBin::current(), "tag3", "delete-tag3-again", 100)
                .is_err()
        );
        assert_eq!(
            repo.all_tags()?.map(|(n, _)| n).collect::<HashSet<_>>(),
            ["tag1", "tag2"].into()
        );
        Ok(())
    }

    #[test]
    fn test_repo_info_updates() -> Result<(), Box<dyn std::error::Error>> {
        let id1 = SnapshotId::random();
        let snap1 = SnapshotInfo {
            id: id1.clone(),
            parent_id: None,
            // needs to be micro second rounded
            flushed_at: DateTime::from_timestamp_micros(1_000_000).unwrap(),
            message: "snap 1".to_string(),
            metadata: Default::default(),
        };

        let num_updates_per_file: u16 = 10;
        let n = num_updates_per_file as usize;

        // check updates for a new repo
        let mut repo = RepoInfo::initial(
            SpecVersionBin::current(),
            snap1,
            num_updates_per_file,
            None,
            None,
        );
        assert_eq!(repo.latest_updates()?.count(), 1);
        let (last_update, _, file) = repo.latest_updates()?.next().unwrap()?;
        assert!(file.is_none());
        assert_eq!(last_update, UpdateType::RepoInitializedUpdate);
        assert!(repo.repo_before_updates()?.is_none());
        // check updates after num_updates_per_file changes
        // fill the first page of updates by adding branches
        for i in 1..=(n - 1) {
            repo = repo.add_branch(
                SpecVersionBin::current(),
                i.to_string().as_str(),
                &id1,
                (i - 1).to_string().as_str(),
                num_updates_per_file,
            )?;
        }

        assert_eq!(repo.latest_updates()?.count(), n);
        let updates = repo.latest_updates()?;

        // check all other updates
        for (idx, update) in updates.enumerate() {
            let (update, _, file) = update?;
            if idx == n - 1 {
                assert_eq!(update, UpdateType::RepoInitializedUpdate);
                assert_eq!(file, Some("0"));
            } else {
                assert_eq!(
                    update,
                    UpdateType::BranchCreatedUpdate { name: (n - 1 - idx).to_string() }
                );
                if idx == 0 {
                    assert!(file.is_none());
                } else {
                    assert_eq!(file, Some((n - 1 - idx).to_string().as_str()));
                }
            }
        }
        assert!(repo.repo_before_updates()?.is_none());

        // Now, if we add another change, it won't fit in the first "page" of repo updates
        repo = repo.add_tag(
            SpecVersionBin::current(),
            "tag",
            &id1,
            "first-branches",
            num_updates_per_file,
        )?;
        // the file only contains the first "page" worth of updates
        assert_eq!(repo.latest_updates()?.count(), n);
        // next file is the oldest change
        assert_eq!(repo.repo_before_updates()?, Some("0"));
        let mut updates = repo.latest_updates()?;
        // last change is the tag creation
        let (last_update, _, file) = updates.next().unwrap()?;
        assert_eq!(last_update, UpdateType::TagCreatedUpdate { name: "tag".to_string() });
        assert!(file.is_none());

        // next change is a branch creation backed up to first-branches
        let (last_update, _, file) = updates.next().unwrap()?;
        assert_eq!(
            last_update,
            UpdateType::BranchCreatedUpdate { name: (n - 1).to_string() }
        );
        assert_eq!(file, Some("first-branches"));

        // all other changes are branch creation (repo creation is in the next page)
        for (idx, update) in updates.enumerate() {
            let (update, _, file) = update?;
            assert_eq!(file, Some((n - 2 - idx).to_string().as_str()));
            assert_eq!(
                update,
                UpdateType::BranchCreatedUpdate { name: (n - 2 - idx).to_string() }
            );
        }

        Ok(())
    }

    #[test]
    fn test_update_timestamp_ordering_rejected() -> Result<(), Box<dyn std::error::Error>>
    {
        let flushed_at = DateTime::from_timestamp_micros(1_000_000).unwrap();
        let id1 = SnapshotId::random();
        let snap1 = SnapshotInfo {
            id: id1.clone(),
            parent_id: None,
            flushed_at,
            message: "snap 1".to_string(),
            metadata: Default::default(),
        };
        let repo = RepoInfo::initial(
            SpecVersionBin::current(),
            snap1,
            100,
            None,
            Some(flushed_at),
        );

        // Attempting add_snapshot with a timestamp equal to the top of the ops log
        // should fail
        let id2 = SnapshotId::random();
        let snap2 = SnapshotInfo {
            id: id2.clone(),
            parent_id: Some(id1.clone()),
            flushed_at: DateTime::from_timestamp_micros(1_000_000).unwrap(),
            message: "snap 2".to_string(),
            metadata: Default::default(),
        };
        let result = repo.add_snapshot(
            SpecVersionBin::current(),
            snap2,
            Some("main"),
            UpdateType::NewCommitUpdate {
                branch: "main".to_string(),
                new_snap_id: id2.clone(),
            },
            Some(flushed_at),
            "backup",
            100,
        );
        assert!(matches!(
            result,
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::InvalidUpdateTimestamp { .. },
                ..
            })
        ));

        // Attempting add_snapshot with a timestamp older than the top of the ops log
        // should also fail
        let flushed_at = DateTime::from_timestamp_micros(500_000).unwrap();
        let id3 = SnapshotId::random();
        let snap3 = SnapshotInfo {
            id: id3.clone(),
            parent_id: Some(id1.clone()),
            flushed_at,
            message: "snap 3".to_string(),
            metadata: Default::default(),
        };
        let result = repo.add_snapshot(
            SpecVersionBin::current(),
            snap3,
            Some("main"),
            UpdateType::NewCommitUpdate {
                branch: "main".to_string(),
                new_snap_id: id3.clone(),
            },
            Some(flushed_at),
            "backup",
            100,
        );
        assert!(matches!(
            result,
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::InvalidUpdateTimestamp { .. },
                ..
            })
        ));

        // Attempting add_snapshot with a strictly newer timestamp should succeed
        let flushed_at = DateTime::from_timestamp_micros(2_000_000).unwrap();
        let id4 = SnapshotId::random();
        let snap4 = SnapshotInfo {
            id: id4.clone(),
            parent_id: Some(id1.clone()),
            flushed_at,
            message: "snap 4".to_string(),
            metadata: Default::default(),
        };
        let result = repo.add_snapshot(
            SpecVersionBin::current(),
            snap4,
            Some("main"),
            UpdateType::NewCommitUpdate {
                branch: "main".to_string(),
                new_snap_id: id4.clone(),
            },
            Some(flushed_at),
            "backup",
            100,
        );
        assert!(result.is_ok());

        Ok(())
    }
}
