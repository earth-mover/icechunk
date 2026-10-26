//! Version info, branches, and tags for a repository.

use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
};
use tracing::trace;

use crate::{
    IcechunkFormatError, IcechunkFormatErrorKind, IcechunkResult, SnapshotId,
    flatbuffers::generated,
    format_constants::SpecVersionBin,
    lookup_index_by_key,
    snapshot::{SnapshotInfo, SnapshotProperties},
    writer_features::{FeatureName, WriterFeature, gate_reason},
};

use chrono::{DateTime, Utc};
use flatbuffers::{UnionWIPOffset, VerifierOptions, WIPOffset};
use icechunk_types::ICResultExt as _;

/// Ops-log entries per repo info file when the repository's configuration
/// doesn't set `num_updates_per_repo_info_file`.
pub const DEFAULT_NUM_UPDATES_PER_REPO_INFO_FILE: u16 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepoAvailability {
    Online,
    ReadOnly,
    // The stored Offline (reserved) and undefined values read as ReadOnly,
    // see `StoredRepoStatus::repo_status`; they are never written.
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

/// A `RepoStatus` table exactly as stored. Notice this needs to be interpreted
/// according to spec 2.2
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoredRepoStatus {
    #[serde(serialize_with = "serialize_raw_availability")]
    availability: generated::RepoAvailability,
    set_at: DateTime<Utc>,
    limited_availability_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    underlying: Option<Box<StoredRepoStatus>>,
}

fn serialize_raw_availability<S: serde::Serializer>(
    availability: &generated::RepoAvailability,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match availability.variant_name() {
        Some(name) => serializer.serialize_str(name),
        None => serializer.serialize_u8(availability.0),
    }
}

impl StoredRepoStatus {
    /// Encode `status` in a repo info potentially listing `writer_features`
    pub(crate) fn new(status: &RepoStatus, writer_features: &[FeatureName]) -> Self {
        let own = StoredRepoStatus {
            availability: status.availability.into(),
            set_at: status.set_at,
            limited_availability_reason: status.limited_availability_reason.clone(),
            underlying: None,
        };
        if writer_features.is_empty() {
            own
        } else {
            StoredRepoStatus {
                availability: generated::RepoAvailability::ReadOnly,
                set_at: status.set_at,
                limited_availability_reason: Some(gate_reason(
                    writer_features,
                    status.limited_availability_reason.as_deref(),
                )),
                underlying: Some(Box::new(own)),
            }
        }
    }

    /// Any status as stored, including shapes and values this library never
    /// writes.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn for_tests(
        availability: generated::RepoAvailability,
        set_at: DateTime<Utc>,
        limited_availability_reason: Option<String>,
        underlying: Option<StoredRepoStatus>,
    ) -> Self {
        Self {
            availability,
            set_at,
            limited_availability_reason,
            underlying: underlying.map(Box::new),
        }
    }

    /// The repository's own status: `underlying` when present. `Offline` and
    /// undefined availabilities read as `ReadOnly`
    pub fn repo_status(&self) -> RepoStatus {
        let own = self.underlying.as_deref().unwrap_or(self);
        let stored_reason = own.limited_availability_reason.as_deref();
        let (availability, limited_availability_reason) = match own.availability {
            generated::RepoAvailability::Online => {
                (RepoAvailability::Online, stored_reason.map(String::from))
            }
            generated::RepoAvailability::ReadOnly => {
                (RepoAvailability::ReadOnly, stored_reason.map(String::from))
            }
            raw => {
                let synthetic = match raw.variant_name() {
                    Some(name) => {
                        format!(
                            "availability {name} ({}) is reserved, read as ReadOnly",
                            raw.0
                        )
                    }
                    None => format!("undefined availability {}, read as ReadOnly", raw.0),
                };
                let reason = match stored_reason {
                    Some(stored) => format!("{synthetic}; {stored}"),
                    None => synthetic,
                };
                (RepoAvailability::ReadOnly, Some(reason))
            }
        };
        RepoStatus { availability, set_at: own.set_at, limited_availability_reason }
    }

    fn shape_defects(&self, writer_features: &[FeatureName]) -> Vec<String> {
        let mut defects = Vec::new();
        match (&self.underlying, writer_features.is_empty()) {
            (Some(_), true) => {
                defects
                    .push("status.underlying is present without writer features".into());
            }
            (None, false) => {
                defects.push("status.underlying is absent with writer features".into());
            }
            _ => {}
        }
        if let Some(underlying) = self.underlying.as_deref() {
            if underlying.underlying.is_some() {
                defects.push("status.underlying has an underlying of its own".into());
            }
            if self.availability != generated::RepoAvailability::ReadOnly {
                defects.push(format!(
                    "the gate's availability is {:?}, not ReadOnly",
                    self.availability
                ));
            }
            if self.set_at != underlying.set_at {
                defects.push(format!(
                    "the gate's set_at {} differs from status.underlying.set_at {}",
                    self.set_at, underlying.set_at
                ));
            }
        }
        if !writer_features.is_sorted() {
            defects.push("writer_features is not sorted".into());
        }
        if writer_features.iter().collect::<BTreeSet<_>>().len() != writer_features.len()
        {
            defects.push("writer_features has duplicates".into());
        }
        defects
    }

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> WIPOffset<generated::RepoStatus<'bldr>> {
        let underlying = self.underlying.as_deref().map(|u| u.to_fb(builder));
        let limited_availability_reason =
            self.limited_availability_reason.as_deref().map(|r| builder.create_string(r));
        generated::RepoStatus::create(
            builder,
            &generated::RepoStatusArgs {
                availability: self.availability,
                set_at: self.set_at.timestamp_micros() as u64,
                limited_availability_reason,
                underlying,
            },
        )
    }
}

impl TryFrom<generated::RepoStatus<'_>> for StoredRepoStatus {
    type Error = IcechunkFormatError;

    fn try_from(fb_status: generated::RepoStatus<'_>) -> Result<Self, Self::Error> {
        Ok(StoredRepoStatus {
            availability: fb_status.availability(),
            set_at: timestamp_to_timestamp(fb_status.set_at())?,
            limited_availability_reason: fb_status
                .limited_availability_reason()
                .map(|s| s.to_string()),
            underlying: fb_status
                .underlying()
                .map(|u| u.try_into().map(Box::new))
                .transpose()?,
        })
    }
}

/// What every rebuild of a repo info carries over: the writer features exactly
/// as listed, and the status with its gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoHeader {
    writer_features: Vec<FeatureName>,
    status: StoredRepoStatus,
}

impl RepoHeader {
    /// The header of a new repo info.
    pub fn new(writer_features: &[WriterFeature], status: &RepoStatus) -> Self {
        let names: BTreeSet<FeatureName> =
            writer_features.iter().copied().map(FeatureName::from).collect();
        Self::from_names(names.into_iter().collect(), status)
    }

    fn from_names(writer_features: Vec<FeatureName>, status: &RepoStatus) -> Self {
        Self { status: StoredRepoStatus::new(status, &writer_features), writer_features }
    }

    /// The same writer features with a new status, and the gate regenerated.
    fn with_status(self, status: &RepoStatus) -> Self {
        Self::from_names(self.writer_features, status)
    }

    pub fn writer_features(&self) -> &[FeatureName] {
        &self.writer_features
    }

    pub fn status(&self) -> &StoredRepoStatus {
        &self.status
    }
}

/// The change [`RepoInfo::check_writable_shape`] checks a repo info for.
///
/// The shape is how the status and the writer features are stored: features
/// sorted and unique, and a `ReadOnly` gate over an underlying status exactly
/// when features are listed. A malformed shape is readable, but only a
/// status-only change may write over it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritableShapeMode {
    /// Any change but a status-only one: the shape must be well formed.
    AnyChange,
    /// A change of the status alone: any shape passes, since the change
    /// rewrites the status well formed. Checking that every listed feature is
    /// implemented is the caller's job.
    StatusOnlyChange,
}

impl RepoStatus {
    pub fn error_msg(&self) -> String {
        format!(
            "Repo status is {0:?}, set at {1}, reason: {2:?}",
            self.availability, self.set_at, self.limited_availability_reason
        )
    }
}

// TODO: should we not implement serialize and let the session fetch the repo info?
#[derive(PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RepoInfoDeserializer")]
pub struct RepoInfo {
    buffer: Vec<u8>,
}

/// Deserialization goes through [`RepoInfo::from_buffer`] so that a `RepoInfo`
/// can never hold an unverified buffer, which the unchecked accessors rely on.
#[derive(Deserialize)]
struct RepoInfoDeserializer {
    buffer: Vec<u8>,
}

impl TryFrom<RepoInfoDeserializer> for RepoInfo {
    type Error = IcechunkFormatError;

    fn try_from(value: RepoInfoDeserializer) -> Result<Self, Self::Error> {
        RepoInfo::from_buffer(value.buffer)
    }
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
        let snaps = self.snapshot_and_parent_ids().unwrap_or_default();
        let snaps = snaps
            .into_iter()
            .map(|(id, parent_id)| {
                format!(
                    "{} -> {}",
                    id,
                    parent_id.map(|s| s.to_string()).unwrap_or_default()
                )
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
        /// The new status, as stored
        status: StoredRepoStatus,
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
    max_tables: 50_000_000,
    // Large enough that any buffer the builder can produce is readable: the
    // builder refuses to grow past FLATBUFFERS_MAX_BUFFER_SIZE, and the
    // verifier's counter runs at most MAX_APPARENT_SIZE_INFLATION times the
    // buffer it walks. Saturating because the product doesn't fit a 32 bit
    // usize (wasm32), where it lands on usize::MAX instead.
    max_apparent_size: crate::serializers::MAX_APPARENT_SIZE_INFLATION
        .saturating_mul(flatbuffers::FLATBUFFERS_MAX_BUFFER_SIZE),
    ignore_missing_null_terminator: true,
};

/// The verifier limits applied to every repo info buffer we read.
#[cfg(test)]
pub(crate) fn root_options() -> &'static VerifierOptions {
    &ROOT_OPTIONS
}

#[derive(Debug, Clone)]
pub struct UpdateInfo<I> {
    pub update_type: UpdateType,
    pub update_time: DateTime<Utc>,
    pub previous_updates: I,
}

pub(crate) type UpdateTuple<'a> =
    IcechunkResult<(UpdateType, DateTime<Utc>, Option<&'a str>)>;

type LatestUpdatesResult<'bldr> = IcechunkResult<(
    WIPOffset<
        flatbuffers::Vector<
            'bldr,
            flatbuffers::ForwardsUOffset<generated::Update<'bldr>>,
        >,
    >,
    Option<WIPOffset<&'bldr str>>,
)>;

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
        header: &RepoHeader,
    ) -> IcechunkResult<Self> {
        let mut snapshots: Vec<_> = snapshots.into_iter().collect();
        snapshots.sort_by_key(|a| a.id.0);
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
            |builder| {
                Self::mk_latest_updates(
                    builder,
                    update,
                    backup_path,
                    num_updates_per_file,
                    previous_info,
                )
            },
            config_bytes,
            sorted_enabled_feature_flags,
            sorted_disabled_feature_flags,
            header,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn from_parts<
        'a,
        F: for<'bldr> FnOnce(
            &mut flatbuffers::FlatBufferBuilder<'bldr>,
        ) -> LatestUpdatesResult<'bldr>,
        EFFIt: DoubleEndedIterator<Item = u16> + ExactSizeIterator,
        DFFIt: DoubleEndedIterator<Item = u16> + ExactSizeIterator,
    >(
        spec_version: SpecVersionBin,
        sorted_tags: impl IntoIterator<Item = (&'a str, u32)>,
        sorted_branches: impl IntoIterator<Item = (&'a str, u32)>,
        sorted_deleted_tags: impl IntoIterator<Item = &'a str>,
        sorted_snapshots: impl IntoIterator<Item = SnapshotInfo>,
        metadata: &SnapshotProperties,
        build_updates: F,
        config_bytes: Option<&[u8]>,
        sorted_enabled_feature_flags: Option<EFFIt>,
        sorted_disabled_feature_flags: Option<DFFIt>,
        header: &RepoHeader,
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
                if name == icechunk_types::DEFAULT_BRANCH {
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
                branch: icechunk_types::DEFAULT_BRANCH.to_string(),
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

                // Absent vector when empty, so unexpired snapshots stay
                // byte-identical to the pre-feature encoding.
                let pruned_ancestor_tx_logs = if snap.pruned_ancestor_tx_logs.is_empty() {
                    None
                } else {
                    let ids: Vec<_> = snap
                        .pruned_ancestor_tx_logs
                        .iter()
                        .map(|id| generated::ObjectId12::new(&id.0))
                        .collect();
                    Some(builder.create_vector(ids.as_slice()))
                };
                debug_assert_eq!(
                    pruned_ancestor_tx_logs.is_none(),
                    snap.pruned_ancestor_tx_logs.is_empty(),
                    "empty pruned_ancestor_tx_logs must serialize as a null vector, \
                     not a present empty one"
                );

                let args = generated::SnapshotInfoArgs {
                    id: Some(&id),
                    parent_offset,
                    flushed_at: snap.flushed_at.timestamp_micros() as u64,
                    message: Some(builder.create_string(snap.message.as_str())),
                    metadata: Some(metadata),
                    pruned_ancestor_tx_logs,
                };
                Ok::<_, IcechunkFormatError>(generated::SnapshotInfo::create(
                    &mut builder,
                    &args,
                ))
            })
            .try_collect()?;
        let snapshots = builder.create_vector(&snapshots);

        let status = header.status.to_fb(&mut builder);
        let writer_features = (!header.writer_features.is_empty()).then(|| {
            let names: Vec<_> = header
                .writer_features
                .iter()
                .map(|name| builder.create_string(name.as_str()))
                .collect();
            builder.create_vector(&names)
        });

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

        let (latest_updates, repo_before_updates) = build_updates(&mut builder)?;

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
            writer_features,
            ..Default::default()
        };
        let repo = generated::Repo::create(&mut builder, &repo_args);
        builder.finish(repo, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Ok(Self { buffer })
    }

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
    ) -> LatestUpdatesResult<'bldr> {
        // replace the backup path in the last update, that must be None, by the new backup path
        let mut previous_updates = update.previous_updates.into_iter();
        let last_update = previous_updates
            .next()
            .map(|maybe_data| {
                let (ut, dt, path) = maybe_data?;
                if path.is_some() {
                    return Err(IcechunkFormatErrorKind::MalformedRepoInfo(
                        "the newest ops log entry has a backup path".to_string(),
                    ))
                    .capture();
                }
                Ok((ut, dt, backup_path))
            })
            .transpose()?
            .map(Ok);
        // A backup_path points to the previous repo info file. It is only meaningful
        // when there are previous updates (you can't back up what doesn't exist).
        // However, previous updates CAN exist without a backup_path: during migration,
        // synthetic ops log entries are generated with no prior repo info file to
        // reference. (This differs from RepoInitializedUpdate, which has neither
        // previous updates nor a backup path.)
        if backup_path.is_some() && last_update.is_none() {
            return Err(IcechunkFormatErrorKind::MalformedRepoInfo(
                "the ops log has no entries".to_string(),
            ))
            .capture();
        }

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

    fn mk_latest_updates_passthrough<'bldr, 'a>(
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
        updates: impl IntoIterator<Item = UpdateTuple<'a>>,
        repo_before_updates: Option<&'a str>,
    ) -> LatestUpdatesResult<'bldr> {
        let mut fb_updates = Vec::new();
        for maybe_data in updates {
            let (u_type, u_time, file) = maybe_data?;
            let (update_type_type, update_type) = update_type_to_fb(builder, &u_type)?;
            let file = file.map(|file| builder.create_string(file));
            fb_updates.push(generated::Update::create(
                builder,
                &generated::UpdateArgs {
                    update_type_type,
                    update_type: Some(update_type),
                    updated_at: u_time.timestamp_micros() as u64,
                    backup_path: file,
                },
            ));
        }
        if fb_updates.is_empty() {
            return Err(IcechunkFormatErrorKind::MalformedRepoInfo(
                "the ops log has no entries".to_string(),
            ))
            .capture();
        }
        let updates = builder.create_vector(&fb_updates);
        let repo_before_updates = repo_before_updates.map(|s| builder.create_string(s));
        Ok((updates, repo_before_updates))
    }

    pub fn initial<C: Serialize>(
        spec_version: SpecVersionBin,
        snapshot: SnapshotInfo,
        num_updates_per_file: u16,
        config: Option<&C>,
        update_time: Option<DateTime<Utc>>,
        writer_features: &[WriterFeature],
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
            |builder| {
                Self::mk_latest_updates(
                    builder,
                    UpdateInfo {
                        update_type: UpdateType::RepoInitializedUpdate,
                        update_time: update_time.unwrap_or_else(Utc::now),
                        previous_updates: [],
                    },
                    None,
                    num_updates_per_file,
                    None,
                )
            },
            config_bytes.as_deref(),
            None::<std::iter::Empty<u16>>,
            None::<std::iter::Empty<u16>>,
            &RepoHeader::new(
                writer_features,
                &RepoStatus {
                    availability: RepoAvailability::Online,
                    set_at: update_time.unwrap_or_else(Utc::now),
                    limited_availability_reason: None,
                },
            ),
        )
        .expect("Cannot generate initial snapshot")
    }

    /// Read the raw config bytes from the `FlatBuffer` (for pass-through in mutations).
    pub fn config_bytes_raw(&self) -> IcechunkResult<Option<Vec<u8>>> {
        Ok(self.root()?.config().map(|v| v.bytes().to_vec()))
    }

    /// The `num_updates_per_repo_info_file` key of the stored configuration,
    /// `None` when there is no configuration or the key is unset. Reads only
    /// this key, so other keys this library can't decode don't matter. A value
    /// it can't decode (wrong type, out of range, or a configuration that
    /// isn't a map) reads as unset, with a warning (spec 2.2).
    pub fn num_updates_per_repo_info_file(&self) -> IcechunkResult<Option<u16>> {
        use flexbuffers::{FlexBufferType, ReaderError};
        const KEY: &str = "num_updates_per_repo_info_file";
        let undecodable = |reason: String| {
            tracing::warn!(
                "ignoring the repository configuration key `{KEY}`, this Icechunk library can't decode its value: {reason}"
            );
            None
        };
        let Some(config) = self.root()?.config() else {
            return Ok(None);
        };
        let config = match flexbuffers::Reader::get_root(config.bytes())
            .and_then(|root| root.get_map())
        {
            Ok(config) => config,
            Err(e) => {
                return Ok(undecodable(format!("the configuration isn't a map: {e}")));
            }
        };
        let value = match config.index(KEY) {
            Ok(value) => value,
            Err(ReaderError::KeyNotFound) => return Ok(None),
            Err(e) => return Ok(undecodable(e.to_string())),
        };
        let wrong_value = |found: String| {
            undecodable(format!(
                "expected an integer from 0 to {}, found {found}",
                u16::MAX
            ))
        };
        let n = match value.flexbuffer_type() {
            FlexBufferType::Null => return Ok(None),
            FlexBufferType::UInt => value.get_u64().map(i128::from),
            FlexBufferType::Int => value.get_i64().map(i128::from),
            other => return Ok(wrong_value(format!("{other:?}"))),
        };
        Ok(match n {
            Ok(n) => u16::try_from(n).ok().or_else(|| wrong_value(n.to_string())),
            Err(e) => undecodable(e.to_string()),
        })
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

    /// The repository's own status: the gate's `underlying` when present.
    /// A malformed repo info reads as `ReadOnly`, with a reason naming the
    /// defects before any other reason.
    pub fn status(&self) -> IcechunkResult<RepoStatus> {
        let raw = self.raw_status()?;
        let status = raw.repo_status();
        let defects = raw.shape_defects(&self.raw_writer_features()?);
        if defects.is_empty() {
            return Ok(status);
        }
        let malformed = format!("malformed repo info: {}", defects.join("; "));
        let reason = match status.limited_availability_reason {
            Some(reason) => format!("{malformed}; {reason}"),
            None => malformed,
        };
        Ok(RepoStatus {
            availability: RepoAvailability::ReadOnly,
            set_at: status.set_at,
            limited_availability_reason: Some(reason),
        })
    }

    /// The status as stored, the gate when the repo info lists writer features.
    pub fn raw_status(&self) -> IcechunkResult<StoredRepoStatus> {
        self.root()?.status().try_into()
    }

    /// The writer features exactly as listed, empty when absent.
    pub fn raw_writer_features(&self) -> IcechunkResult<Vec<FeatureName>> {
        Ok(self
            .root()?
            .writer_features()
            .map(|names| names.iter().map(FeatureName::new).collect())
            .unwrap_or_default())
    }

    /// The listed writer features, sorted and without duplicates.
    pub fn writer_features(&self) -> IcechunkResult<BTreeSet<FeatureName>> {
        Ok(self.raw_writer_features()?.into_iter().collect())
    }

    /// The header every rebuild of this repo info copies.
    pub fn header(&self) -> IcechunkResult<RepoHeader> {
        Ok(RepoHeader {
            writer_features: self.raw_writer_features()?,
            status: self.raw_status()?,
        })
    }

    /// A [`IcechunkFormatErrorKind::MalformedRepoInfo`] error if this repo
    /// info is malformed, judged on the status and the writer features as
    /// stored, unless `mode` allows it. A malformed repo info is still
    /// readable.
    pub fn check_writable_shape(&self, mode: WritableShapeMode) -> IcechunkResult<()> {
        match mode {
            WritableShapeMode::StatusOnlyChange => Ok(()),
            WritableShapeMode::AnyChange => {
                let defects =
                    self.raw_status()?.shape_defects(&self.raw_writer_features()?);
                if defects.is_empty() {
                    Ok(())
                } else {
                    Err(IcechunkFormatErrorKind::MalformedRepoInfo(defects.join("; ")))
                        .capture()
                }
            }
        }
    }

    /// The same repo info with `writer_features` and `status` stored exactly
    /// as given, including malformed shapes and values this library never
    /// writes.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn with_raw_header_for_tests(
        &self,
        writer_features: Vec<FeatureName>,
        status: StoredRepoStatus,
    ) -> IcechunkResult<Self> {
        let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
        let latest_updates = self.latest_updates()?;
        let rbu = self.repo_before_updates()?;
        Self::from_parts(
            self.spec_version()?,
            self.all_tags()?,
            self.all_branches()?,
            self.deleted_tags()?,
            snaps,
            &self.metadata()?,
            |builder| Self::mk_latest_updates_passthrough(builder, latest_updates, rbu),
            self.config_bytes_raw()?.as_deref(),
            self.enabled_feature_flags()?,
            self.disabled_feature_flags()?,
            &RepoHeader { writer_features, status },
        )
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

    /// Reads the ids from the flatbuffer. It does not deserialize snapshot metadata.
    fn snapshot_and_parent_ids(
        &self,
    ) -> IcechunkResult<Vec<(SnapshotId, Option<SnapshotId>)>> {
        let root = self.root()?;
        root.snapshots()
            .iter()
            .map(|snap| {
                let parent_id = parent_index(&root, &snap)?
                    .map(|ix| snapshot_at(&root, ix).map(|p| SnapshotId::new(p.id().0)))
                    .transpose()?;
                Ok((SnapshotId::new(snap.id().0), parent_id))
            })
            .try_collect()
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

        let latest_updates = self.latest_updates()?;
        let rbu = self.repo_before_updates()?;
        Self::from_parts(
            spec_version,
            self.all_tags()?,
            self.all_branches()?,
            self.deleted_tags()?,
            snaps,
            &self.metadata()?,
            |builder| {
                Self::mk_latest_updates(
                    builder,
                    UpdateInfo {
                        update_type: UpdateType::FeatureFlagChanged {
                            id: flag_id,
                            new_value: enabled,
                        },
                        update_time: Utc::now(),
                        previous_updates: latest_updates,
                    },
                    Some(previous_file),
                    num_updates_per_file,
                    rbu,
                )
            },
            self.config_bytes_raw()?.as_deref(),
            eff.map(|it| it.into_iter()),
            dff.map(|it| it.into_iter()),
            &self.header()?,
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

        let latest_updates = self.latest_updates()?;
        let rbu = self.repo_before_updates()?;
        let res = Self::from_parts(
            spec_version,
            tags,
            branches,
            self.deleted_tags()?,
            snapshots,
            &self.metadata()?,
            |builder| {
                Self::mk_latest_updates(
                    builder,
                    UpdateInfo {
                        update_type,
                        update_time: update_time.unwrap_or_else(Utc::now),
                        previous_updates: latest_updates,
                    },
                    Some(previous_file),
                    num_updates_per_file,
                    rbu,
                )
            },
            self.config_bytes_raw()?.as_deref(),
            self.enabled_feature_flags()?,
            self.disabled_feature_flags()?,
            &self.header()?,
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
                branches.sort_by_key(|(name1, _)| *name1);
                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                let latest_updates = self.latest_updates()?;
                let rbu = self.repo_before_updates()?;
                Ok(Self::from_parts(
                    spec_version,
                    self.all_tags()?,
                    branches,
                    self.deleted_tags()?,
                    snaps,
                    &self.metadata()?,
                    |builder| {
                        Self::mk_latest_updates(
                            builder,
                            UpdateInfo {
                                update_type: UpdateType::BranchCreatedUpdate {
                                    name: name.to_string(),
                                },
                                update_time: Utc::now(),
                                previous_updates: latest_updates,
                            },
                            Some(previous_file),
                            num_updates_per_file,
                            rbu,
                        )
                    },
                    self.config_bytes_raw()?.as_deref(),
                    self.enabled_feature_flags()?,
                    self.disabled_feature_flags()?,
                    &self.header()?,
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
                let latest_updates = self.latest_updates()?;
                let rbu = self.repo_before_updates()?;
                Self::from_parts(
                    spec_version,
                    self.all_tags()?,
                    branches,
                    self.deleted_tags()?,
                    snaps,
                    &self.metadata()?,
                    |builder| {
                        Self::mk_latest_updates(
                            builder,
                            UpdateInfo {
                                update_type: UpdateType::BranchDeletedUpdate {
                                    name: name.to_string(),
                                    previous_snap_id,
                                },
                                update_time: Utc::now(),
                                previous_updates: latest_updates,
                            },
                            Some(previous_file),
                            num_updates_per_file,
                            rbu,
                        )
                    },
                    self.config_bytes_raw()?.as_deref(),
                    self.enabled_feature_flags()?,
                    self.disabled_feature_flags()?,
                    &self.header()?,
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
                let latest_updates = self.latest_updates()?;
                let rbu = self.repo_before_updates()?;
                Ok(Self::from_parts(
                    spec_version,
                    self.all_tags()?,
                    branches,
                    self.deleted_tags()?,
                    snaps,
                    &self.metadata()?,
                    |builder| {
                        Self::mk_latest_updates(
                            builder,
                            UpdateInfo {
                                update_type: UpdateType::BranchResetUpdate {
                                    name: name.to_string(),
                                    previous_snap_id,
                                },
                                update_time: Utc::now(),
                                previous_updates: latest_updates,
                            },
                            Some(previous_file),
                            num_updates_per_file,
                            rbu,
                        )
                    },
                    self.config_bytes_raw()?.as_deref(),
                    self.enabled_feature_flags()?,
                    self.disabled_feature_flags()?,
                    &self.header()?,
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
                tags.sort_by_key(|(name1, _)| *name1);
                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                let latest_updates = self.latest_updates()?;
                let rbu = self.repo_before_updates()?;
                Ok(Self::from_parts(
                    spec_version,
                    tags,
                    self.all_branches()?,
                    self.deleted_tags()?,
                    snaps,
                    &self.metadata()?,
                    |builder| {
                        Self::mk_latest_updates(
                            builder,
                            UpdateInfo {
                                update_type: UpdateType::TagCreatedUpdate {
                                    name: name.to_string(),
                                },
                                update_time: Utc::now(),
                                previous_updates: latest_updates,
                            },
                            Some(previous_file),
                            num_updates_per_file,
                            rbu,
                        )
                    },
                    self.config_bytes_raw()?.as_deref(),
                    self.enabled_feature_flags()?,
                    self.disabled_feature_flags()?,
                    &self.header()?,
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
                let latest_updates = self.latest_updates()?;
                let rbu = self.repo_before_updates()?;
                Self::from_parts(
                    spec_version,
                    tags,
                    self.all_branches()?,
                    deleted_tags,
                    snaps,
                    &self.metadata()?,
                    |builder| {
                        Self::mk_latest_updates(
                            builder,
                            UpdateInfo {
                                update_type: UpdateType::TagDeletedUpdate {
                                    name: name.to_string(),
                                    previous_snap_id,
                                },
                                update_time: Utc::now(),
                                previous_updates: latest_updates,
                            },
                            Some(previous_file),
                            num_updates_per_file,
                            rbu,
                        )
                    },
                    self.config_bytes_raw()?.as_deref(),
                    self.enabled_feature_flags()?,
                    self.disabled_feature_flags()?,
                    &self.header()?,
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
        let latest_updates = self.latest_updates()?;
        let rbu = self.repo_before_updates()?;
        Self::from_parts(
            spec_version,
            self.all_tags()?,
            self.all_branches()?,
            self.deleted_tags()?,
            snaps,
            metadata,
            |builder| {
                Self::mk_latest_updates(
                    builder,
                    UpdateInfo {
                        update_type: UpdateType::MetadataChangedUpdate,
                        update_time: Utc::now(),
                        previous_updates: latest_updates,
                    },
                    Some(previous_file),
                    num_updates_per_file,
                    rbu,
                )
            },
            self.config_bytes_raw()?.as_deref(),
            self.enabled_feature_flags()?,
            self.disabled_feature_flags()?,
            &self.header()?,
        )
    }

    /// Update the embedded configuration and record a `ConfigChangedUpdate` in the op log.
    pub fn set_config<C: Serialize>(
        &self,
        spec_version: SpecVersionBin,
        config: &C,
        previous_file: &str,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        let config_bytes = flexbuffers::to_vec(config).map_err(Box::new).capture()?;
        let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
        let latest_updates = self.latest_updates()?;
        let rbu = self.repo_before_updates()?;
        Self::from_parts(
            spec_version,
            self.all_tags()?,
            self.all_branches()?,
            self.deleted_tags()?,
            snaps,
            &self.metadata()?,
            |builder| {
                Self::mk_latest_updates(
                    builder,
                    UpdateInfo {
                        update_type: UpdateType::ConfigChangedUpdate,
                        update_time: Utc::now(),
                        previous_updates: latest_updates,
                    },
                    Some(previous_file),
                    num_updates_per_file,
                    rbu,
                )
            },
            Some(config_bytes.as_slice()),
            self.enabled_feature_flags()?,
            self.disabled_feature_flags()?,
            &self.header()?,
        )
    }

    /// Set the repository status.
    ///
    /// When `update_config` is `Some((previous_file, num_updates_per_file))`, a
    /// `RepoStatusChangedUpdate` entry is appended to the ops log (normal behavior).
    ///
    /// When `update_config` is `None`, the status is changed silently: the existing
    /// ops log entries are preserved as-is with no new entry added.
    pub fn set_status(
        &self,
        spec_version: SpecVersionBin,
        status: &RepoStatus,
        update_config: Option<(&str, u16)>,
    ) -> IcechunkResult<Self> {
        let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
        let latest_updates = self.latest_updates()?;
        let rbu = self.repo_before_updates()?;
        let header = self.header()?.with_status(status);
        if let Some((previous_file, num_updates_per_file)) = update_config {
            let status_update = header.status.clone();
            Self::from_parts(
                spec_version,
                self.all_tags()?,
                self.all_branches()?,
                self.deleted_tags()?,
                snaps,
                &self.metadata()?,
                |builder| {
                    Self::mk_latest_updates(
                        builder,
                        UpdateInfo {
                            update_type: UpdateType::RepoStatusChangedUpdate {
                                status: status_update,
                            },
                            update_time: Utc::now(),
                            previous_updates: latest_updates,
                        },
                        Some(previous_file),
                        num_updates_per_file,
                        rbu,
                    )
                },
                self.config_bytes_raw()?.as_deref(),
                self.enabled_feature_flags()?,
                self.disabled_feature_flags()?,
                &header,
            )
        } else {
            Self::from_parts(
                spec_version,
                self.all_tags()?,
                self.all_branches()?,
                self.deleted_tags()?,
                snaps,
                &self.metadata()?,
                |builder| {
                    Self::mk_latest_updates_passthrough(builder, latest_updates, rbu)
                },
                self.config_bytes_raw()?.as_deref(),
                self.enabled_feature_flags()?,
                self.disabled_feature_flags()?,
                &header,
            )
        }
    }

    /// Add and remove writer features, keeping the status.
    /// Appends a `RepoStatusChangedUpdate` at `update_time`
    /// carrying the full new status, and refuses a change that changes nothing.
    pub fn with_writer_features(
        &self,
        spec_version: SpecVersionBin,
        add: &[WriterFeature],
        remove: &[WriterFeature],
        previous_file: &str,
        update_time: DateTime<Utc>,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        let names = |features: &[WriterFeature]| -> Vec<FeatureName> {
            features.iter().copied().map(FeatureName::from).collect()
        };
        self.change_writer_features(
            spec_version,
            &names(add),
            &names(remove),
            previous_file,
            update_time,
            num_updates_per_file,
        )
    }

    /// [`RepoInfo::with_writer_features`] for any name that follows the
    /// specification's grammar, such as features this implementation doesn't
    /// define.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn with_writer_features_for_tests(
        &self,
        spec_version: SpecVersionBin,
        add: &[FeatureName],
        remove: &[FeatureName],
        previous_file: &str,
        update_time: DateTime<Utc>,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        for name in add {
            FeatureName::parse(name.as_str())?;
        }
        self.change_writer_features(
            spec_version,
            add,
            remove,
            previous_file,
            update_time,
            num_updates_per_file,
        )
    }

    fn change_writer_features(
        &self,
        spec_version: SpecVersionBin,
        add: &[FeatureName],
        remove: &[FeatureName],
        previous_file: &str,
        update_time: DateTime<Utc>,
        num_updates_per_file: u16,
    ) -> IcechunkResult<Self> {
        let invalid = |msg: String| {
            Err(IcechunkFormatErrorKind::InvalidWriterFeatureChange(msg)).capture()
        };
        if add.is_empty() && remove.is_empty() {
            return invalid("no writer feature to add or remove".to_string());
        }
        let listed = self.writer_features()?;
        let mut features = listed.clone();
        for name in remove {
            if !features.remove(name) {
                return invalid(format!("{name} is not listed"));
            }
        }
        for name in add {
            if listed.contains(name) || !features.insert(name.clone()) {
                return invalid(format!("{name} is already listed"));
            }
        }

        let header =
            RepoHeader::from_names(features.into_iter().collect(), &self.status()?);
        let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
        let latest_updates = self.latest_updates()?;
        let rbu = self.repo_before_updates()?;
        Self::from_parts(
            spec_version,
            self.all_tags()?,
            self.all_branches()?,
            self.deleted_tags()?,
            snaps,
            &self.metadata()?,
            |builder| {
                Self::mk_latest_updates(
                    builder,
                    UpdateInfo {
                        update_type: UpdateType::RepoStatusChangedUpdate {
                            status: header.status.clone(),
                        },
                        update_time,
                        previous_updates: latest_updates,
                    },
                    Some(previous_file),
                    num_updates_per_file,
                    rbu,
                )
            },
            self.config_bytes_raw()?.as_deref(),
            self.enabled_feature_flags()?,
            self.disabled_feature_flags()?,
            &header,
        )
    }

    /// Check that `buffer` passes every check [`RepoInfo::from_buffer`] applies.
    pub fn verify_buffer(buffer: &[u8]) -> IcechunkResult<()> {
        let _ = flatbuffers::root_with_opts::<generated::Repo<'_>>(&ROOT_OPTIONS, buffer)
            .capture()?;
        Ok(())
    }

    pub fn from_buffer(buffer: Vec<u8>) -> IcechunkResult<RepoInfo> {
        Self::verify_buffer(buffer.as_slice())?;
        Ok(RepoInfo { buffer })
    }

    pub fn bytes(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    #[expect(unsafe_code)]
    fn root(&self) -> IcechunkResult<generated::Repo<'_>> {
        // SAFETY: every RepoInfo holds a verified buffer: `from_buffer` and the
        // `Deserialize` impl verify, and `from_parts` builds with our own
        // serialization code. We skip validation here because these accessors are
        // called many times per repo info, and each check is O(file).
        Ok(unsafe { flatbuffers::root_unchecked::<generated::Repo<'_>>(&self.buffer) })
    }

    pub fn tag_names(&self) -> IcechunkResult<impl Iterator<Item = &str>> {
        Ok(self.root()?.tags().iter().map(|r| r.name()))
    }

    pub fn branch_names(&self) -> IcechunkResult<impl Iterator<Item = &str>> {
        Ok(self.root()?.branches().iter().map(|r| r.name()))
    }

    pub fn tags(&self) -> IcechunkResult<impl Iterator<Item = (&str, SnapshotId)>> {
        self.resolve_refs(self.all_tags()?)
    }

    pub fn branches(&self) -> IcechunkResult<impl Iterator<Item = (&str, SnapshotId)>> {
        self.resolve_refs(self.all_branches()?)
    }

    fn resolve_refs<'a>(
        &'a self,
        refs: impl Iterator<Item = (&'a str, u32)>,
    ) -> IcechunkResult<impl Iterator<Item = (&'a str, SnapshotId)>> {
        let root = self.root()?;
        let resolved: Vec<_> = refs
            .map(|(name, idx)| {
                let snap = snapshot_at(&root, idx as usize)?;
                Ok::<_, IcechunkFormatError>((name, SnapshotId::new(snap.id().0)))
            })
            .try_collect()?;
        Ok(resolved.into_iter())
    }

    pub fn resolve_tag(&self, name: &str) -> IcechunkResult<SnapshotId> {
        let root = self.root()?;
        let res = root
            .tags()
            .lookup_by_key(name, |r, key| r.name().cmp(key))
            .ok_or_else(|| IcechunkFormatErrorKind::TagNotFound { tag: name.to_string() })
            .capture()?;
        let snap = snapshot_at(&root, res.snapshot_index() as usize)?;
        Ok(SnapshotId::new(snap.id().0))
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
            .ok_or_else(|| IcechunkFormatErrorKind::BranchNotFound {
                branch: name.to_string(),
            })
            .capture()?;
        let snap = snapshot_at(&root, res.snapshot_index() as usize)?;
        Ok(SnapshotId::new(snap.id().0))
    }

    /// The spec version in the `Repo` table: only 2 is valid (spec 1
    /// repositories have no repo info).
    pub fn spec_version(&self) -> IcechunkResult<SpecVersionBin> {
        let raw = self.root()?.spec_version();
        match raw {
            n if n == SpecVersionBin::V2 as u8 => Ok(SpecVersionBin::V2),
            n if n < SpecVersionBin::V2 as u8 => {
                Err(IcechunkFormatErrorKind::InvalidRepoInfoSpecVersion { found: n })
                    .capture()
            }
            n => Err(IcechunkFormatErrorKind::InvalidSpecVersion {
                found: n,
                max_supported: SpecVersionBin::current() as u8,
            })
            .capture(),
        }
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
        // The verifier guarantees a table for every defined union tag, so
        // these accessors return `Some`; a format error keeps it panic free.
        fn table<T>(table: Option<T>) -> IcechunkResult<T> {
            table
                .ok_or_else(|| {
                    IcechunkFormatErrorKind::InvalidFlatBuffer(
                        flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                            field: Cow::Borrowed("update_type_type"),
                            field_type: Cow::Borrowed("UpdateType"),
                            error_trace: Default::default(),
                        },
                    )
                })
                .capture()
        }
        match update.update_type_type() {
            generated::UpdateType::RepoInitializedUpdate => {
                Ok(UpdateType::RepoInitializedUpdate)
            }
            generated::UpdateType::RepoMigratedUpdate => {
                let up = table(update.update_type_as_repo_migrated_update())?;
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
                let up = table(update.update_type_as_repo_status_changed_update())?;
                let status = up
                    .status()
                    .ok_or_else(|| {
                        IcechunkFormatErrorKind::InvalidFlatBuffer(
                            flatbuffers::InvalidFlatbuffer::MissingRequiredField {
                                required: Cow::Borrowed("RepoStatusChangedUpdate.status"),
                                error_trace: Default::default(),
                            },
                        )
                    })
                    .capture()?
                    .try_into()?;

                Ok(UpdateType::RepoStatusChangedUpdate { status })
            }
            generated::UpdateType::ConfigChangedUpdate => {
                Ok(UpdateType::ConfigChangedUpdate)
            }
            generated::UpdateType::MetadataChangedUpdate => {
                Ok(UpdateType::MetadataChangedUpdate)
            }
            generated::UpdateType::TagCreatedUpdate => {
                let up = table(update.update_type_as_tag_created_update())?;
                Ok(UpdateType::TagCreatedUpdate { name: up.name().to_string() })
            }
            generated::UpdateType::TagDeletedUpdate => {
                let up = table(update.update_type_as_tag_deleted_update())?;
                let previous_snap_id = SnapshotId::new(up.previous_snap_id().0);
                Ok(UpdateType::TagDeletedUpdate {
                    name: up.name().to_string(),
                    previous_snap_id,
                })
            }
            generated::UpdateType::BranchCreatedUpdate => {
                let up = table(update.update_type_as_branch_created_update())?;
                Ok(UpdateType::BranchCreatedUpdate { name: up.name().to_string() })
            }
            generated::UpdateType::BranchDeletedUpdate => {
                let up = table(update.update_type_as_branch_deleted_update())?;
                let previous_snap_id = SnapshotId::new(up.previous_snap_id().0);
                Ok(UpdateType::BranchDeletedUpdate {
                    name: up.name().to_string(),
                    previous_snap_id,
                })
            }
            generated::UpdateType::BranchResetUpdate => {
                let up = table(update.update_type_as_branch_reset_update())?;
                let previous_snap_id = SnapshotId::new(up.previous_snap_id().0);
                Ok(UpdateType::BranchResetUpdate {
                    name: up.name().to_string(),
                    previous_snap_id,
                })
            }
            generated::UpdateType::NewCommitUpdate => {
                let up = table(update.update_type_as_new_commit_update())?;
                let new_snap_id = SnapshotId::new(up.new_snap_id().0);
                Ok(UpdateType::NewCommitUpdate {
                    branch: up.branch().to_string(),
                    new_snap_id,
                })
            }
            generated::UpdateType::CommitAmendedUpdate => {
                let up = table(update.update_type_as_commit_amended_update())?;
                let previous_snap_id = SnapshotId::new(up.previous_snap_id().0);
                let new_snap_id = SnapshotId::new(up.new_snap_id().0);
                Ok(UpdateType::CommitAmendedUpdate {
                    branch: up.branch().to_string(),
                    previous_snap_id,
                    new_snap_id,
                })
            }
            generated::UpdateType::NewDetachedSnapshotUpdate => {
                let up = table(update.update_type_as_new_detached_snapshot_update())?;
                let new_snap_id = SnapshotId::new(up.new_snap_id().0);
                Ok(UpdateType::NewDetachedSnapshotUpdate { new_snap_id })
            }
            generated::UpdateType::GCRanUpdate => Ok(UpdateType::GCRanUpdate),
            generated::UpdateType::ExpirationRanUpdate => {
                Ok(UpdateType::ExpirationRanUpdate)
            }
            generated::UpdateType::FeatureFlagChangedUpdate => {
                let up = table(update.update_type_as_feature_flag_changed_update())?;
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
            let mut next = Some(start);
            // a chain without a cycle visits every snapshot at most once
            let mut steps_left = root.snapshots().len();
            let iter = std::iter::from_fn(move || {
                let ix = next.take()?;
                if steps_left == 0 {
                    return Some(
                        Err(IcechunkFormatErrorKind::MalformedRepoInfo(
                            "the snapshot parents form a cycle".to_string(),
                        ))
                        .capture(),
                    );
                }
                steps_left -= 1;
                let snap = snapshot_at(&root, ix).and_then(|snap| {
                    next = parent_index(&root, &snap)?;
                    mk_snapshot_info(&root, &snap)
                });
                Some(snap)
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
        // ancestry found the snapshot, and it is always the first element
        self.ancestry(id)?.next().unwrap_or_else(|| {
            Err(IcechunkFormatErrorKind::SnapshotIdNotFound { snapshot_id: id.clone() })
                .capture()
        })
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
    res.sort_by_key(|(name1, _)| *name1);
    Ok(res)
}

fn timestamp_to_timestamp(ts: u64) -> IcechunkResult<DateTime<Utc>> {
    let ts: i64 =
        ts.try_into().map_err(|_| IcechunkFormatErrorKind::InvalidTimestamp).capture()?;
    DateTime::from_timestamp_micros(ts)
        .ok_or(IcechunkFormatErrorKind::InvalidTimestamp)
        .capture()
}

/// The snapshot at a stored index, a format error when it is out of range.
fn snapshot_at<'a>(
    repo: &generated::Repo<'a>,
    index: usize,
) -> IcechunkResult<generated::SnapshotInfo<'a>> {
    let snapshots = repo.snapshots();
    if index < snapshots.len() {
        Ok(snapshots.get(index))
    } else {
        Err(IcechunkFormatErrorKind::MalformedRepoInfo(format!(
            "snapshot index {index} is out of range, the repo info has {} snapshots",
            snapshots.len()
        )))
        .capture()
    }
}

/// The index of `snap`'s parent: `None` for -1, a format error for any other
/// negative offset or one out of range.
fn parent_index(
    repo: &generated::Repo<'_>,
    snap: &generated::SnapshotInfo<'_>,
) -> IcechunkResult<Option<usize>> {
    match snap.parent_offset() {
        -1 => Ok(None),
        offset => usize::try_from(offset)
            .ok()
            .filter(|ix| *ix < repo.snapshots().len())
            .map(Some)
            .ok_or_else(|| {
                IcechunkFormatErrorKind::MalformedRepoInfo(format!(
                    "snapshot {} has parent offset {offset}, the repo info has {} snapshots",
                    SnapshotId::new(snap.id().0),
                    repo.snapshots().len()
                ))
            })
            .capture(),
    }
}

fn mk_snapshot_info(
    repo: &generated::Repo<'_>,
    snap: &generated::SnapshotInfo<'_>,
) -> IcechunkResult<SnapshotInfo> {
    let flushed_at = timestamp_to_timestamp(snap.flushed_at())?;
    let parent_id = parent_index(repo, snap)?
        .map(|ix| snapshot_at(repo, ix).map(|parent| parent.id()))
        .transpose()?;
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

    let pruned_ancestor_tx_logs = snap
        .pruned_ancestor_tx_logs()
        .map(|ids| ids.iter().map(|oid| SnapshotId::new(oid.0)).collect())
        .unwrap_or_default();

    Ok(SnapshotInfo {
        id: SnapshotId::new(snap.id().0),
        flushed_at,
        message: snap.message().to_string(),
        metadata,
        parent_id: parent_id.map(|buf| SnapshotId::new(buf.0)),
        pruned_ancestor_tx_logs,
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
            let status = status.to_fb(builder);

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

    fn a_snapshot_info(message: &str) -> SnapshotInfo {
        SnapshotInfo {
            id: SnapshotId::random(),
            parent_id: None,
            flushed_at: DateTime::from_timestamp_micros(1_000_000).unwrap(),
            message: message.to_string(),
            metadata: Default::default(),
            pruned_ancestor_tx_logs: vec![],
        }
    }

    // Every RepoInfo holds a buffer that was verified, either on read or when we
    // built it, which is what makes the unchecked accessors sound.
    fn valid_repo_info() -> impl Strategy<Value = RepoInfo> {
        proptest::collection::vec(any::<String>(), 1..5).prop_map(|messages| {
            RepoInfo::initial(
                SpecVersionBin::current(),
                a_snapshot_info(messages.first().map_or("initial", |m| m.as_str())),
                100,
                None::<&()>,
                None,
                &[],
            )
        })
    }

    roundtrip_serialization_tests!(serialize_and_deserialize_repo_info - valid_repo_info);

    #[test]
    fn deserializing_an_unverified_buffer_fails() {
        // a RepoInfo whose buffer is not a repo file at all
        let bytes = rmp_serde::to_vec(&RepoInfo { buffer: vec![0u8; 64] }).unwrap();
        assert!(
            rmp_serde::from_slice::<RepoInfo>(bytes.as_slice()).is_err(),
            "deserialization must not produce a RepoInfo with an unverified buffer"
        );
    }

    #[test]
    fn repo_info_with_more_than_ten_million_snapshots_is_usable()
    -> Result<(), Box<dyn std::error::Error>> {
        // flatbuffers' default verifier options cap a buffer at 1M tables, and
        // every snapshot is one table. Repos this size must still be readable.
        let n = 10_000_001;
        let snapshots: Vec<_> = (0..n).map(|_| a_snapshot_info("")).collect();
        let branch_target = snapshots[0].id.clone();
        let info = RepoInfo::new(
            SpecVersionBin::current(),
            [],
            [("main", branch_target)],
            [],
            snapshots,
            &Default::default(),
            UpdateInfo {
                update_type: UpdateType::RepoInitializedUpdate,
                update_time: Utc::now(),
                previous_updates: Vec::<UpdateTuple<'_>>::new(),
            },
            None,
            10,
            None,
            None,
            None::<std::iter::Empty<u16>>,
            None::<std::iter::Empty<u16>>,
            &RepoHeader::new(
                &[],
                &RepoStatus {
                    availability: RepoAvailability::Online,
                    set_at: Utc::now(),
                    limited_availability_reason: None,
                },
            ),
        )?;

        let info = RepoInfo::from_buffer(info.bytes().to_vec())?;
        assert_eq!(info.all_snapshots()?.count(), n);
        assert!(info.branch_names()?.any(|name| name == "main"));
        Ok(())
    }

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
            pruned_ancestor_tx_logs: vec![],
        };
        let repo = RepoInfo::initial(
            SpecVersionBin::current(),
            snap1.clone(),
            100,
            None::<&()>,
            None,
            &[],
        );
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
    fn test_pruned_ancestor_tx_logs_roundtrip() -> Result<(), Box<dyn std::error::Error>>
    {
        let root_id = SnapshotId::random();
        let root = SnapshotInfo {
            id: root_id.clone(),
            parent_id: None,
            flushed_at: DateTime::from_timestamp_micros(1_000_000).unwrap(),
            message: "root".to_string(),
            metadata: Default::default(),
            pruned_ancestor_tx_logs: vec![],
        };
        let repo = RepoInfo::initial(
            SpecVersionBin::current(),
            root.clone(),
            100,
            None::<&()>,
            None,
            &[],
        );

        // an edited snapshot carrying a non-empty list
        let pruned =
            vec![SnapshotId::random(), SnapshotId::random(), SnapshotId::random()];
        let tip_id = SnapshotId::random();
        let tip = SnapshotInfo {
            id: tip_id.clone(),
            parent_id: Some(root_id.clone()),
            flushed_at: DateTime::from_timestamp_micros(2_000_000).unwrap(),
            message: "tip".to_string(),
            metadata: Default::default(),
            pruned_ancestor_tx_logs: pruned.clone(),
        };
        let repo = repo.add_snapshot(
            SpecVersionBin::current(),
            tip.clone(),
            Some("main"),
            UpdateType::NewCommitUpdate {
                branch: "main".to_string(),
                new_snap_id: tip_id.clone(),
            },
            None,
            "foo",
            100,
        )?;

        // the list survives serialization verbatim, order preserved
        let read_tip = repo.find_snapshot(&tip_id)?;
        assert_eq!(read_tip.pruned_ancestor_tx_logs, pruned);

        // an unedited snapshot reads back as empty (absent vector)
        let read_root = repo.find_snapshot(&root_id)?;
        assert!(read_root.pruned_ancestor_tx_logs.is_empty());
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
            pruned_ancestor_tx_logs: vec![],
        };
        let repo = RepoInfo::initial(
            SpecVersionBin::current(),
            snap1.clone(),
            100,
            None::<&()>,
            None,
            &[],
        );
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
            pruned_ancestor_tx_logs: vec![],
        };

        let num_updates_per_file: u16 = 10;
        let n = num_updates_per_file as usize;

        // check updates for a new repo
        let mut repo = RepoInfo::initial(
            SpecVersionBin::current(),
            snap1,
            num_updates_per_file,
            None::<&()>,
            None,
            &[],
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
            pruned_ancestor_tx_logs: vec![],
        };
        let repo = RepoInfo::initial(
            SpecVersionBin::current(),
            snap1,
            100,
            None::<&()>,
            Some(flushed_at),
            &[],
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
            pruned_ancestor_tx_logs: vec![],
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
            pruned_ancestor_tx_logs: vec![],
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
            pruned_ancestor_tx_logs: vec![],
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

    /// Verify that `set_status` with `update_config = None` (silent mode)
    /// changes the repo status without appending an entry to the ops log.
    #[test]
    fn test_set_status_silent() -> Result<(), Box<dyn std::error::Error>> {
        let id1 = SnapshotId::random();
        let snap1 = SnapshotInfo {
            id: id1.clone(),
            parent_id: None,
            flushed_at: DateTime::from_timestamp_micros(1_000_000).unwrap(),
            message: "snap 1".to_string(),
            metadata: Default::default(),
            pruned_ancestor_tx_logs: vec![],
        };

        let repo = RepoInfo::initial(
            SpecVersionBin::current(),
            snap1,
            100,
            None::<&()>,
            None,
            &[],
        );

        // Add a branch to have more than one ops log entry
        let repo =
            repo.add_branch(SpecVersionBin::current(), "dev", &id1, "backup0", 100)?;

        let updates_before: Vec<_> =
            repo.latest_updates()?.collect::<Result<Vec<_>, _>>()?;
        let rbu_before = repo.repo_before_updates()?;
        let num_updates_before = updates_before.len();

        // Normal set_status: should add a RepoStatusChangedUpdate entry
        let new_status = RepoStatus {
            availability: RepoAvailability::ReadOnly,
            set_at: Utc::now(),
            limited_availability_reason: Some("maintenance".to_string()),
        };
        let repo_normal = repo.set_status(
            SpecVersionBin::current(),
            &new_status,
            Some(("backup1", 100)),
        )?;
        let updates_after_normal: Vec<_> =
            repo_normal.latest_updates()?.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(updates_after_normal.len(), num_updates_before + 1);
        assert!(matches!(
            &updates_after_normal[0].0,
            UpdateType::RepoStatusChangedUpdate { .. }
        ));
        let normal_status = repo_normal.status()?;
        assert_eq!(normal_status.availability, RepoAvailability::ReadOnly);
        assert_eq!(
            normal_status.limited_availability_reason.as_deref(),
            Some("maintenance")
        );

        // Silent set_status: should NOT add an ops log entry
        let silent_status = RepoStatus {
            availability: RepoAvailability::ReadOnly,
            set_at: Utc::now(),
            limited_availability_reason: None,
        };
        let repo_silent =
            repo.set_status(SpecVersionBin::current(), &silent_status, None)?;

        let updates_after_silent: Vec<_> =
            repo_silent.latest_updates()?.collect::<Result<Vec<_>, _>>()?;
        // Ops log unchanged: same number of entries, same content
        assert_eq!(updates_after_silent.len(), num_updates_before);
        for (before, after) in updates_before.iter().zip(updates_after_silent.iter()) {
            assert_eq!(before.0, after.0);
            assert_eq!(before.1, after.1);
            assert_eq!(before.2, after.2);
        }
        assert_eq!(repo_silent.repo_before_updates()?, rbu_before);

        // But the status itself IS changed
        let silent_result_status = repo_silent.status()?;
        assert_eq!(silent_result_status.availability, RepoAvailability::ReadOnly);
        assert!(silent_result_status.limited_availability_reason.is_none());

        Ok(())
    }

    fn fs_layout() -> Vec<FeatureName> {
        vec![FeatureName::from(WriterFeature::FilesystemLayoutV1)]
    }

    fn read_only(reason: &str) -> RepoStatus {
        RepoStatus {
            availability: RepoAvailability::ReadOnly,
            set_at: DateTime::from_timestamp_micros(5_000_000).unwrap(),
            limited_availability_reason: Some(reason.to_string()),
        }
    }

    fn assert_gate_shape(info: &RepoInfo) -> Result<(), Box<dyn std::error::Error>> {
        let features = info.raw_writer_features()?;
        let raw = info.raw_status()?;
        if features.is_empty() {
            assert!(raw.underlying.is_none());
        } else {
            let underlying = raw.underlying.as_deref().expect("a gate wraps the status");
            assert_eq!(raw.availability, generated::RepoAvailability::ReadOnly);
            assert_eq!(raw.set_at, underlying.set_at);
            assert!(underlying.underlying.is_none(), "underlying is never nested twice");
            assert_eq!(
                raw.limited_availability_reason.as_deref(),
                Some(
                    gate_reason(
                        &features,
                        underlying.limited_availability_reason.as_deref()
                    )
                    .as_str()
                )
            );
        }
        Ok(())
    }

    /// A repo info listing `filesystem-layout-v1`, with a branch `dev`, a tag
    /// `t` and a `ReadOnly` status with a reason.
    fn gated_repo() -> Result<(RepoInfo, SnapshotId), Box<dyn std::error::Error>> {
        let snap1 = a_snapshot_info("snap 1");
        let id1 = snap1.id.clone();
        let repo = RepoInfo::initial(
            SpecVersionBin::current(),
            snap1,
            100,
            None::<&()>,
            None,
            &[WriterFeature::FilesystemLayoutV1],
        );
        let repo = repo.add_branch(SpecVersionBin::current(), "dev", &id1, "b0", 100)?;
        let repo = repo.add_tag(SpecVersionBin::current(), "t", &id1, "b1", 100)?;
        let repo = repo.set_status(
            SpecVersionBin::current(),
            &read_only("maintenance"),
            Some(("b2", 100)),
        )?;
        Ok((repo, id1))
    }

    fn status_entries(
        info: &RepoInfo,
    ) -> Result<Vec<StoredRepoStatus>, Box<dyn std::error::Error>> {
        Ok(info
            .latest_updates()?
            .map_ok(|(update, _, _)| update)
            .filter_map_ok(|update| match update {
                UpdateType::RepoStatusChangedUpdate { status } => Some(status),
                _ => None,
            })
            .try_collect()?)
    }

    type Mutator = Box<dyn Fn(&RepoInfo) -> IcechunkResult<RepoInfo>>;

    #[test]
    fn every_mutator_keeps_writer_features_and_gate()
    -> Result<(), Box<dyn std::error::Error>> {
        let (base, id1) = gated_repo()?;
        assert_eq!(base.raw_writer_features()?, fs_layout());
        assert_gate_shape(&base)?;
        let v = SpecVersionBin::current();
        let snap2 = SnapshotInfo { parent_id: Some(id1.clone()), ..a_snapshot_info("2") };
        let new_status = RepoStatus {
            availability: RepoAvailability::Online,
            set_at: DateTime::from_timestamp_micros(9_000_000).unwrap(),
            limited_availability_reason: None,
        };
        let id = id1.clone();
        let mutators: Vec<(&str, Mutator)> = vec![
            (
                "update_feature_flag",
                Box::new(move |r| r.update_feature_flag(v, 3, Some(false), "bk", 100)),
            ),
            (
                "add_snapshot",
                Box::new(move |r| {
                    r.add_snapshot(
                        v,
                        snap2.clone(),
                        Some("main"),
                        UpdateType::NewCommitUpdate {
                            branch: "main".to_string(),
                            new_snap_id: snap2.id.clone(),
                        },
                        None,
                        "bk",
                        100,
                    )
                }),
            ),
            ("add_branch", Box::new(move |r| r.add_branch(v, "other", &id, "bk", 100))),
            ("delete_branch", Box::new(move |r| r.delete_branch(v, "dev", "bk", 100))),
            (
                "update_branch",
                Box::new({
                    let id = id1.clone();
                    move |r| r.update_branch(v, "dev", &id, "bk", 100)
                }),
            ),
            (
                "add_tag",
                Box::new({
                    let id = id1.clone();
                    move |r| r.add_tag(v, "t2", &id, "bk", 100)
                }),
            ),
            ("delete_tag", Box::new(move |r| r.delete_tag(v, "t", "bk", 100))),
            (
                "set_metadata",
                Box::new(move |r| {
                    r.set_metadata(
                        v,
                        &[("k".to_string(), serde_json::json!(1))].into(),
                        "bk",
                        100,
                    )
                }),
            ),
            (
                "set_config",
                Box::new(move |r| {
                    r.set_config(v, &HashMap::from([("k", 1u32)]), "bk", 100)
                }),
            ),
            (
                "set_status",
                Box::new({
                    let s = new_status.clone();
                    move |r| r.set_status(v, &s, Some(("bk", 100)))
                }),
            ),
        ];
        assert_eq!(mutators.len(), 10);

        for (name, mutate) in mutators {
            let after = RepoInfo::from_buffer(mutate(&base)?.bytes().to_vec())?;
            assert_eq!(after.raw_writer_features()?, fs_layout(), "{name}");
            assert_gate_shape(&after)?;
            if name == "set_status" {
                assert_eq!(after.status()?, new_status);
                assert_eq!(status_entries(&after)?[0], after.raw_status()?);
            } else {
                assert_eq!(after.header()?, base.header()?, "{name}");
                assert_eq!(after.status()?, read_only("maintenance"), "{name}");
            }
        }
        Ok(())
    }

    #[test]
    fn gate_round_trip_and_raw_shape() -> Result<(), Box<dyn std::error::Error>> {
        let at = DateTime::from_timestamp_micros(3_000_000).unwrap();
        let plain = RepoInfo::initial(
            SpecVersionBin::current(),
            a_snapshot_info("1"),
            100,
            None::<&()>,
            Some(at),
            &[],
        );
        assert!(plain.raw_writer_features()?.is_empty());
        assert!(plain.root()?.writer_features().is_none(), "never an empty vector");
        assert_eq!(plain.raw_status()?.underlying, None);
        assert_gate_shape(&plain)?;

        let gated = RepoInfo::initial(
            SpecVersionBin::current(),
            a_snapshot_info("1"),
            100,
            None::<&()>,
            Some(at),
            &[WriterFeature::FilesystemLayoutV1],
        );
        let gated = RepoInfo::from_buffer(gated.bytes().to_vec())?;
        assert_eq!(gated.raw_writer_features()?, fs_layout());
        assert_eq!(gated.writer_features()?, fs_layout().into_iter().collect());
        assert_gate_shape(&gated)?;
        let raw = gated.raw_status()?;
        assert_eq!(raw.set_at, at);
        assert_eq!(
            raw.underlying.as_deref().map(|u| u.availability),
            Some(generated::RepoAvailability::Online)
        );
        assert_eq!(
            gated.status()?,
            RepoStatus {
                availability: RepoAvailability::Online,
                set_at: at,
                limited_availability_reason: None
            }
        );
        // creation is not a change of writer features
        assert_eq!(gated.latest_updates()?.count(), 1);

        let gated = gated.set_status(
            SpecVersionBin::current(),
            &read_only("why"),
            Some(("b", 100)),
        )?;
        let gated = RepoInfo::from_buffer(gated.bytes().to_vec())?;
        assert_eq!(gated.status()?, read_only("why"));
        assert_gate_shape(&gated)?;
        Ok(())
    }

    #[test]
    fn with_writer_features_add_then_remove() -> Result<(), Box<dyn std::error::Error>> {
        let v = SpecVersionBin::current();
        let repo = RepoInfo::initial(
            v,
            a_snapshot_info("1"),
            100,
            None::<&()>,
            Some(DateTime::from_timestamp_micros(1_000_000).unwrap()),
            &[],
        );
        let repo = repo.set_status(v, &read_only("r"), Some(("b0", 100)))?;
        let entries_before = repo.latest_updates()?.count();

        // whole microseconds, as stored
        let t1 =
            DateTime::from_timestamp_micros(Utc::now().timestamp_micros() + 1_000_000)
                .unwrap();
        let added = repo.with_writer_features(
            v,
            &[WriterFeature::FilesystemLayoutV1],
            &[],
            "b-add",
            t1,
            100,
        )?;
        let added = RepoInfo::from_buffer(added.bytes().to_vec())?;
        assert_eq!(added.raw_writer_features()?, fs_layout());
        assert_eq!(added.status()?, read_only("r"));
        assert_gate_shape(&added)?;
        let updates: Vec<_> = added.latest_updates()?.try_collect()?;
        assert_eq!(updates.len(), entries_before + 1);
        let (entry, entry_time, entry_backup) = &updates[0];
        assert_eq!(
            entry,
            &UpdateType::RepoStatusChangedUpdate { status: added.raw_status()? }
        );
        assert_eq!(*entry_time, t1);
        assert_eq!(*entry_backup, None);
        assert_eq!(updates[1].2, Some("b-add"));
        // the entry's status equals the previous one: a change of writer features
        let UpdateType::RepoStatusChangedUpdate { status } = entry else {
            unreachable!()
        };
        assert_eq!(status.repo_status(), repo.status()?);

        let t2 = t1 + chrono::Duration::seconds(1);
        let removed = added.with_writer_features(
            v,
            &[],
            &[WriterFeature::FilesystemLayoutV1],
            "b-remove",
            t2,
            100,
        )?;
        let removed = RepoInfo::from_buffer(removed.bytes().to_vec())?;
        assert!(removed.raw_writer_features()?.is_empty());
        assert!(removed.root()?.writer_features().is_none());
        assert_eq!(removed.raw_status()?.underlying, None);
        assert_eq!(removed.status()?, read_only("r"));
        let updates: Vec<_> = removed.latest_updates()?.try_collect()?;
        assert_eq!(updates.len(), entries_before + 2);
        assert_eq!(
            updates[0].0,
            UpdateType::RepoStatusChangedUpdate { status: removed.raw_status()? }
        );
        assert_eq!(updates[1].2, Some("b-remove"));
        assert_eq!(updates[2].2, Some("b-add"));

        let t3 = t2 + chrono::Duration::seconds(1);
        let no_change = |r: IcechunkResult<RepoInfo>| {
            matches!(
                r,
                Err(IcechunkFormatError {
                    kind: IcechunkFormatErrorKind::InvalidWriterFeatureChange(_),
                    ..
                })
            )
        };
        assert!(no_change(added.with_writer_features(
            v,
            &[WriterFeature::FilesystemLayoutV1],
            &[],
            "b",
            t3,
            100
        )));
        assert!(no_change(removed.with_writer_features(
            v,
            &[],
            &[WriterFeature::FilesystemLayoutV1],
            "b",
            t3,
            100
        )));
        assert!(no_change(removed.with_writer_features(v, &[], &[], "b", t3, 100)));
        Ok(())
    }

    #[test]
    fn gated_history_survives_unrelated_changes() -> Result<(), Box<dyn std::error::Error>>
    {
        let (repo, id1) = gated_repo()?;
        let v = SpecVersionBin::current();
        let mut repo = repo.set_status(
            v,
            &RepoStatus {
                availability: RepoAvailability::Online,
                set_at: DateTime::from_timestamp_micros(6_000_000).unwrap(),
                limited_availability_reason: None,
            },
            Some(("b3", 100)),
        )?;
        let history = status_entries(&repo)?;
        assert_eq!(history.len(), 2);
        assert!(history.iter().all(|s| s.underlying.is_some()));

        for i in 0..10 {
            repo = repo.add_branch(v, &format!("br{i}"), &id1, &format!("bk{i}"), 100)?;
        }
        let repo = RepoInfo::from_buffer(repo.bytes().to_vec())?;
        assert_eq!(status_entries(&repo)?, history);
        Ok(())
    }

    #[test]
    fn raw_writer_features_are_copied_unchanged() -> Result<(), Box<dyn std::error::Error>>
    {
        let v = SpecVersionBin::current();
        let snap = a_snapshot_info("1");
        let id = snap.id.clone();
        let raw = vec![
            FeatureName::new("b-v1"),
            FeatureName::new("a-v1"),
            FeatureName::new("a-v1"),
        ];
        let header = RepoHeader {
            status: StoredRepoStatus::new(&read_only("r"), &raw),
            writer_features: raw.clone(),
        };
        let repo = RepoInfo::new(
            v,
            [],
            [("main", id.clone())],
            [],
            [snap],
            &Default::default(),
            UpdateInfo {
                update_type: UpdateType::RepoInitializedUpdate,
                update_time: DateTime::from_timestamp_micros(1_000_000).unwrap(),
                previous_updates: Vec::<UpdateTuple<'_>>::new(),
            },
            None,
            100,
            None,
            None,
            None::<std::iter::Empty<u16>>,
            None::<std::iter::Empty<u16>>,
            &header,
        )?;
        let fb_names = |r: &RepoInfo| -> Vec<Vec<u8>> {
            r.root()
                .unwrap()
                .writer_features()
                .unwrap()
                .iter()
                .map(|s| s.as_bytes().to_vec())
                .collect()
        };
        let changed = repo.add_branch(v, "dev", &id, "bk", 100)?;
        assert_eq!(fb_names(&changed), fb_names(&repo));
        assert_eq!(changed.raw_writer_features()?, raw);
        assert_eq!(changed.header()?, repo.header()?);
        assert_eq!(
            changed.writer_features()?,
            [FeatureName::new("a-v1"), FeatureName::new("b-v1")].into()
        );
        Ok(())
    }

    #[test]
    fn with_writer_features_for_tests_checks_the_grammar()
    -> Result<(), Box<dyn std::error::Error>> {
        let v = SpecVersionBin::current();
        let repo = RepoInfo::initial(
            v,
            a_snapshot_info("1"),
            100,
            None::<&()>,
            Some(DateTime::from_timestamp_micros(1_000_000).unwrap()),
            &[],
        );
        let now = Utc::now();
        for bad in ["Test-Feature-v1", "org.example.feature-v1", "feature"] {
            assert!(matches!(
                repo.with_writer_features_for_tests(
                    v,
                    &[FeatureName::new(bad)],
                    &[],
                    "b",
                    now,
                    100
                ),
                Err(IcechunkFormatError {
                    kind: IcechunkFormatErrorKind::InvalidWriterFeatureName { .. },
                    ..
                })
            ));
        }
        let test_feature = FeatureName::parse("test-feature-v1")?;
        let with_test = repo.with_writer_features_for_tests(
            v,
            std::slice::from_ref(&test_feature),
            &[],
            "b",
            now,
            100,
        )?;
        assert_eq!(with_test.raw_writer_features()?, vec![test_feature]);
        assert_gate_shape(&with_test)?;
        assert!(
            with_test
                .raw_status()?
                .limited_availability_reason
                .unwrap()
                .contains("require a newer Icechunk")
        );
        Ok(())
    }

    fn at(micros: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_micros(micros).unwrap()
    }

    fn stored(
        availability: u8,
        set_at: DateTime<Utc>,
        reason: Option<&str>,
        underlying: Option<StoredRepoStatus>,
    ) -> StoredRepoStatus {
        StoredRepoStatus::for_tests(
            generated::RepoAvailability(availability),
            set_at,
            reason.map(String::from),
            underlying,
        )
    }

    fn plain_repo() -> RepoInfo {
        RepoInfo::initial(
            SpecVersionBin::current(),
            a_snapshot_info("1"),
            100,
            None::<&()>,
            Some(at(1_000_000)),
            &[],
        )
    }

    fn is_malformed<T>(result: &IcechunkResult<T>) -> bool {
        matches!(
            result,
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::MalformedRepoInfo(_),
                ..
            })
        )
    }

    /// Readable, and read as not `Online`, with a one line reason without
    /// double quotes.
    fn assert_reads_as_malformed(
        info: &RepoInfo,
        name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let status = info.status()?;
        assert_eq!(status.availability, RepoAvailability::ReadOnly, "{name}");
        assert_eq!(status.set_at, info.raw_status()?.repo_status().set_at, "{name}");
        let reason = status.limited_availability_reason.unwrap_or_default();
        assert!(reason.starts_with("malformed repo info: "), "{name}: {reason}");
        assert!(!reason.contains(['"', '\n', '\r']), "{name}: {reason}");
        Ok(())
    }

    #[test]
    fn malformed_shapes_are_readable_but_not_writable()
    -> Result<(), Box<dyn std::error::Error>> {
        let v = SpecVersionBin::current();
        let set_at = at(5_000_000);
        let online = stored(0, set_at, None, None);
        let gate = |availability: u8, gate_set_at, underlying| {
            stored(availability, gate_set_at, Some("gate"), Some(underlying))
        };
        let (a, b) = (FeatureName::new("a-v1"), FeatureName::new("b-v1"));
        let gated = |features: &[FeatureName]| {
            StoredRepoStatus::new(&online.repo_status(), features)
        };
        let malformed_features = ["unsorted features", "duplicate features"];
        let rows: Vec<(&str, Vec<FeatureName>, StoredRepoStatus)> = vec![
            (
                "underlying without features",
                vec![],
                stored(0, set_at, None, Some(online.clone())),
            ),
            (
                "features without underlying",
                fs_layout(),
                stored(1, set_at, Some("r"), None),
            ),
            (
                "double nesting",
                fs_layout(),
                gate(1, set_at, stored(1, set_at, None, Some(online.clone()))),
            ),
            ("gate Online", fs_layout(), gate(0, set_at, online.clone())),
            ("gate Offline", fs_layout(), gate(2, set_at, online.clone())),
            ("gate undefined", fs_layout(), gate(9, set_at, online.clone())),
            ("set_at mismatch", fs_layout(), gate(1, at(6_000_000), online.clone())),
            (
                malformed_features[0],
                vec![b.clone(), a.clone()],
                gated(&[b.clone(), a.clone()]),
            ),
            (
                malformed_features[1],
                vec![a.clone(), a.clone()],
                gated(&[a.clone(), a.clone()]),
            ),
        ];

        for (name, features, status) in rows {
            let info = plain_repo().with_raw_header_for_tests(features, status)?;
            let info = RepoInfo::from_buffer(info.bytes().to_vec())?;
            assert_reads_as_malformed(&info, name)?;
            info.raw_status()?;
            info.latest_updates()?.try_collect::<_, Vec<_>, _>()?;
            assert!(
                is_malformed(&info.check_writable_shape(WritableShapeMode::AnyChange)),
                "{name}"
            );
            info.check_writable_shape(WritableShapeMode::StatusOnlyChange)?;

            let repaired = info.set_status(v, &read_only("fixed"), Some(("b", 100)))?;
            let repaired = RepoInfo::from_buffer(repaired.bytes().to_vec())?;
            assert_gate_shape(&repaired)?;
            let after = repaired.check_writable_shape(WritableShapeMode::AnyChange);
            if malformed_features.contains(&name) {
                // a status-only change copies the features as listed
                assert!(is_malformed(&after), "{name}");
                assert_reads_as_malformed(&repaired, name)?;
                assert!(
                    repaired
                        .status()?
                        .limited_availability_reason
                        .is_some_and(|r| r.ends_with("; fixed")),
                    "{name}"
                );
            } else {
                after?;
                assert_eq!(repaired.status()?, read_only("fixed"), "{name}");
            }
        }

        // the malformed reason comes first, then the Offline one, then the stored one
        let offline_underlying = plain_repo().with_raw_header_for_tests(
            vec![],
            stored(1, set_at, None, Some(stored(2, set_at, Some("down"), None))),
        )?;
        assert_eq!(
            offline_underlying.status()?,
            RepoStatus {
                availability: RepoAvailability::ReadOnly,
                set_at,
                limited_availability_reason: Some(
                    "malformed repo info: status.underlying is present without writer \
                     features; availability Offline (2) is reserved, read as ReadOnly; down"
                        .to_string()
                ),
            }
        );

        let well_formed: Vec<(&str, Vec<FeatureName>, StoredRepoStatus)> = vec![
            ("plain", vec![], online.clone()),
            ("plain Offline", vec![], stored(2, set_at, None, None)),
            ("plain undefined", vec![], stored(9, set_at, None, None)),
            ("gate", fs_layout(), gated(&fs_layout())),
            ("gate with any reason", fs_layout(), gate(1, set_at, online.clone())),
            (
                "gate over Offline",
                fs_layout(),
                gate(1, set_at, stored(2, set_at, None, None)),
            ),
        ];
        for (name, features, status) in well_formed {
            let info = plain_repo().with_raw_header_for_tests(features, status)?;
            let info = RepoInfo::from_buffer(info.bytes().to_vec())?;
            info.check_writable_shape(WritableShapeMode::AnyChange)
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert!(
                !info
                    .status()?
                    .limited_availability_reason
                    .is_some_and(|r| r.starts_with("malformed")),
                "{name}"
            );
        }
        Ok(())
    }

    #[test]
    fn offline_and_undefined_availability_read_as_read_only()
    -> Result<(), Box<dyn std::error::Error>> {
        let set_at = at(5_000_000);
        let cases = [
            (2, None, "availability Offline (2) is reserved, read as ReadOnly"),
            (
                2,
                Some("down"),
                "availability Offline (2) is reserved, read as ReadOnly; down",
            ),
            (9, None, "undefined availability 9, read as ReadOnly"),
            (255, Some("x"), "undefined availability 255, read as ReadOnly; x"),
        ];
        for (raw, reason, expected) in cases {
            let expected = RepoStatus {
                availability: RepoAvailability::ReadOnly,
                set_at,
                limited_availability_reason: Some(expected.to_string()),
            };
            let own = stored(raw, set_at, reason, None);
            let plain = plain_repo().with_raw_header_for_tests(vec![], own.clone())?;
            let plain = RepoInfo::from_buffer(plain.bytes().to_vec())?;
            assert_eq!(plain.status()?, expected);
            assert_eq!(plain.raw_status()?.availability.0, raw);

            let gated = plain_repo().with_raw_header_for_tests(
                fs_layout(),
                stored(1, set_at, Some("gate"), Some(own)),
            )?;
            assert_eq!(gated.status()?, expected);
        }
        Ok(())
    }

    #[test]
    fn writer_feature_change_persists_offline_as_read_only()
    -> Result<(), Box<dyn std::error::Error>> {
        let v = SpecVersionBin::current();
        let set_at = at(5_000_000);
        let offline = plain_repo()
            .with_raw_header_for_tests(vec![], stored(2, set_at, Some("down"), None))?;
        let mapped = offline.status()?;
        assert_eq!(mapped.availability, RepoAvailability::ReadOnly);

        let t1 = at(Utc::now().timestamp_micros() + 1_000_000);
        let added = offline.with_writer_features(
            v,
            &[WriterFeature::FilesystemLayoutV1],
            &[],
            "b-add",
            t1,
            100,
        )?;
        let added = RepoInfo::from_buffer(added.bytes().to_vec())?;
        let underlying = added.raw_status()?.underlying.expect("a gate");
        assert_eq!(underlying.availability, generated::RepoAvailability::ReadOnly);
        assert_eq!(
            underlying.limited_availability_reason,
            mapped.limited_availability_reason
        );
        assert_eq!(underlying.set_at, set_at);
        assert_eq!(added.status()?, mapped);
        assert_eq!(status_entries(&added)?[0], added.raw_status()?);
        added.check_writable_shape(WritableShapeMode::AnyChange)?;

        let removed = added.with_writer_features(
            v,
            &[],
            &[WriterFeature::FilesystemLayoutV1],
            "b-remove",
            t1 + chrono::Duration::seconds(1),
            100,
        )?;
        let raw = removed.raw_status()?;
        assert_eq!(raw.availability, generated::RepoAvailability::ReadOnly);
        assert_eq!(removed.status()?, mapped);
        Ok(())
    }

    #[test]
    fn repo_info_spec_version_must_be_2() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(plain_repo().spec_version()?, SpecVersionBin::V2);
        let v1 = RepoInfo::initial(
            SpecVersionBin::V1,
            a_snapshot_info("1"),
            100,
            None::<&()>,
            None,
            &[],
        );
        let v1 = RepoInfo::from_buffer(v1.bytes().to_vec())?;
        assert!(matches!(
            v1.spec_version(),
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::InvalidRepoInfoSpecVersion { found: 1 },
                ..
            })
        ));
        Ok(())
    }

    type CraftedUpdates = for<'b> fn(
        &mut flatbuffers::FlatBufferBuilder<'b>,
    ) -> Vec<WIPOffset<generated::Update<'b>>>;

    /// A repo info whose ops log is built by `updates`, unverified.
    fn crafted(updates: CraftedUpdates) -> IcechunkResult<RepoInfo> {
        RepoInfo::from_parts(
            SpecVersionBin::current(),
            [],
            [("main", 0)],
            [],
            [a_snapshot_info("1")],
            &Default::default(),
            |builder| {
                let updates = updates(builder);
                Ok((builder.create_vector(&updates), None))
            },
            None,
            None::<std::iter::Empty<u16>>,
            None::<std::iter::Empty<u16>>,
            &RepoHeader::new(&[], &read_only("r")),
        )
    }

    fn update<'b>(
        builder: &mut flatbuffers::FlatBufferBuilder<'b>,
        update_type_type: generated::UpdateType,
        update_type: WIPOffset<UnionWIPOffset>,
        backup_path: Option<&str>,
    ) -> WIPOffset<generated::Update<'b>> {
        let backup_path = backup_path.map(|p| builder.create_string(p));
        generated::Update::create(
            builder,
            &generated::UpdateArgs {
                update_type_type,
                update_type: Some(update_type),
                updated_at: 1_000_000,
                backup_path,
            },
        )
    }

    fn gc_ran(
        builder: &mut flatbuffers::FlatBufferBuilder<'_>,
    ) -> WIPOffset<UnionWIPOffset> {
        generated::GCRanUpdate::create(builder, &generated::GCRanUpdateArgs {})
            .as_union_value()
    }

    #[test]
    fn crafted_ops_log_entries_are_errors_not_panics()
    -> Result<(), Box<dyn std::error::Error>> {
        // a union tag without its table fails verification
        let tag_without_table = crafted(|b| {
            // the generated builder refuses to omit a required field
            let start = b.start_table();
            b.push_slot(
                generated::Update::VT_UPDATE_TYPE_TYPE,
                generated::UpdateType::TagCreatedUpdate,
                generated::UpdateType::NONE,
            );
            b.push_slot(generated::Update::VT_UPDATED_AT, 1_000_000u64, 0);
            vec![WIPOffset::new(b.end_table(start).value())]
        })?;
        assert!(RepoInfo::from_buffer(tag_without_table.bytes().to_vec()).is_err());

        let status_without_status = crafted(|b| {
            let up = generated::RepoStatusChangedUpdate::create(
                b,
                &generated::RepoStatusChangedUpdateArgs { status: None },
            )
            .as_union_value();
            vec![update(b, generated::UpdateType::RepoStatusChangedUpdate, up, None)]
        })?;
        let unknown_tag = crafted(|b| {
            let up = gc_ran(b);
            vec![update(b, generated::UpdateType(200), up, None)]
        })?;
        for info in [status_without_status, unknown_tag] {
            let info = RepoInfo::from_buffer(info.bytes().to_vec())?;
            assert!(info.latest_updates()?.try_collect::<_, Vec<_>, _>().is_err());
        }
        Ok(())
    }

    #[test]
    fn malformed_ops_log_is_an_error_when_changing()
    -> Result<(), Box<dyn std::error::Error>> {
        let v = SpecVersionBin::current();
        let empty = crafted(|_| vec![])?;
        let newest_with_backup = crafted(|b| {
            let up = gc_ran(b);
            vec![update(b, generated::UpdateType::GCRanUpdate, up, Some("bk"))]
        })?;
        for info in [empty, newest_with_backup] {
            let info = RepoInfo::from_buffer(info.bytes().to_vec())?;
            let id = info.resolve_branch("main")?;
            assert!(matches!(
                info.add_branch(v, "dev", &id, "bk2", 100),
                Err(IcechunkFormatError {
                    kind: IcechunkFormatErrorKind::MalformedRepoInfo(_),
                    ..
                })
            ));
        }
        let empty = RepoInfo::from_buffer(crafted(|_| vec![])?.bytes().to_vec())?;
        assert!(matches!(
            empty.set_status(v, &read_only("r2"), None),
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::MalformedRepoInfo(_),
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn deepest_path_of_a_large_gated_ops_log_verifies()
    -> Result<(), Box<dyn std::error::Error>> {
        let v = SpecVersionBin::current();
        let mut repo = RepoInfo::initial(
            v,
            a_snapshot_info("1"),
            1000,
            None::<&()>,
            Some(at(1_000_000)),
            &[WriterFeature::FilesystemLayoutV1],
        );
        for i in 0..999 {
            repo = repo.set_status(
                v,
                &RepoStatus {
                    availability: RepoAvailability::ReadOnly,
                    set_at: at(2_000_000 + i),
                    limited_availability_reason: Some(format!("reason {i}")),
                },
                Some((&format!("b{i}"), 1000)),
            )?;
        }
        let repo = RepoInfo::from_buffer(repo.bytes().to_vec())?;
        let entries = status_entries(&repo)?;
        assert_eq!(repo.latest_updates()?.count(), 1000);
        assert_eq!(entries.len(), 999);
        assert!(entries.iter().all(|s| {
            s.underlying.as_ref().is_some_and(|u| u.limited_availability_reason.is_some())
        }));

        // Repo → Update → RepoStatusChangedUpdate → status → underlying
        let depth = |max_depth| {
            let opts = VerifierOptions { max_depth, ..ROOT_OPTIONS };
            flatbuffers::root_with_opts::<generated::Repo<'_>>(&opts, repo.bytes())
                .is_ok()
        };
        assert!(depth(5));
        assert!(!depth(4));
        assert!(ROOT_OPTIONS.max_depth >= 6, "a malformed double nesting stays readable");
        Ok(())
    }

    fn snapshot_ref<'b>(
        builder: &mut flatbuffers::FlatBufferBuilder<'b>,
        name: &str,
        snapshot_index: u32,
    ) -> WIPOffset<generated::Ref<'b>> {
        let name = builder.create_string(name);
        generated::Ref::create(
            builder,
            &generated::RefArgs { name: Some(name), snapshot_index },
        )
    }

    /// A verified repo info whose snapshots (sorted ids, returned) have the
    /// given parent offsets, with `main` and tag `t` at the given indexes.
    fn crafted_snapshots(
        parent_offsets: &[i32],
        main: u32,
        tag: u32,
    ) -> Result<(RepoInfo, Vec<SnapshotId>), Box<dyn std::error::Error>> {
        let mut ids: Vec<_> =
            parent_offsets.iter().map(|_| SnapshotId::random()).collect();
        ids.sort_by_key(|id| id.0);
        let mut b = flatbuffers::FlatBufferBuilder::new();
        let snapshots: Vec<_> = ids
            .iter()
            .zip(parent_offsets)
            .map(|(id, parent_offset)| {
                let oid = generated::ObjectId12::new(&id.0);
                let message = b.create_string("m");
                generated::SnapshotInfo::create(
                    &mut b,
                    &generated::SnapshotInfoArgs {
                        id: Some(&oid),
                        parent_offset: *parent_offset,
                        flushed_at: 1_000_000,
                        message: Some(message),
                        metadata: None,
                        pruned_ancestor_tx_logs: None,
                    },
                )
            })
            .collect();
        let snapshots = b.create_vector(&snapshots);
        let branches = [snapshot_ref(&mut b, "main", main)];
        let branches = b.create_vector(&branches);
        let tags = [snapshot_ref(&mut b, "t", tag)];
        let tags = b.create_vector(&tags);
        let deleted_tags = b.create_vector::<flatbuffers::ForwardsUOffset<&str>>(&[]);
        let status = StoredRepoStatus::new(&read_only("r"), &[]).to_fb(&mut b);
        let up = generated::RepoInitializedUpdate::create(
            &mut b,
            &generated::RepoInitializedUpdateArgs {},
        )
        .as_union_value();
        let updates =
            [update(&mut b, generated::UpdateType::RepoInitializedUpdate, up, None)];
        let latest_updates = b.create_vector(&updates);
        let repo = generated::Repo::create(
            &mut b,
            &generated::RepoArgs {
                spec_version: SpecVersionBin::V2 as u8,
                tags: Some(tags),
                branches: Some(branches),
                deleted_tags: Some(deleted_tags),
                snapshots: Some(snapshots),
                status: Some(status),
                latest_updates: Some(latest_updates),
                ..Default::default()
            },
        );
        b.finish(repo, Some("Ichk"));
        Ok((RepoInfo::from_buffer(b.finished_data().to_vec())?, ids))
    }

    #[test]
    fn out_of_range_snapshot_indexes_are_errors_not_panics()
    -> Result<(), Box<dyn std::error::Error>> {
        for offset in [1, 5, -2] {
            let (info, ids) = crafted_snapshots(&[offset], 0, 0)?;
            assert!(is_malformed(&info.all_snapshots()?.try_collect::<_, Vec<_>, _>()));
            assert!(is_malformed(&info.ancestry(&ids[0])?.try_collect::<_, Vec<_>, _>()));
            assert!(is_malformed(&info.find_snapshot(&ids[0])));
            assert!(is_malformed(&info.snapshot_and_parent_ids()));
            let _ = format!("{info:?}");
        }

        let (info, _) = crafted_snapshots(&[-1], 1, 0)?;
        assert!(is_malformed(&info.resolve_branch("main")));
        assert!(is_malformed(&info.branches().map(|_| ())));
        assert_eq!(info.tags()?.count(), 1);
        let _ = format!("{info:?}");

        let (info, _) = crafted_snapshots(&[-1], 0, 3)?;
        assert!(is_malformed(&info.resolve_tag("t")));
        assert!(is_malformed(&info.tags().map(|_| ())));
        assert_eq!(info.branches()?.count(), 1);
        let _ = format!("{info:?}");
        Ok(())
    }

    #[test]
    fn snapshot_parent_cycles_are_errors_not_hangs()
    -> Result<(), Box<dyn std::error::Error>> {
        // a self-parent, a two-cycle, and a two-cycle beside a root
        for (offsets, in_cycle) in
            [(vec![0], vec![0]), (vec![1, 0], vec![0, 1]), (vec![-1, 2, 1], vec![1, 2])]
        {
            let (info, ids) = crafted_snapshots(&offsets, 0, 0)?;
            for ix in in_cycle {
                assert!(is_malformed(
                    &info.ancestry(&ids[ix])?.try_collect::<_, Vec<_>, _>()
                ));
                info.find_snapshot(&ids[ix])?;
            }
        }
        let (info, ids) = crafted_snapshots(&[-1, 2, 1], 0, 0)?;
        assert_eq!(info.ancestry(&ids[0])?.try_collect::<_, Vec<_>, _>()?.len(), 1);
        Ok(())
    }

    fn with_config(config: Option<&serde_json::Value>) -> RepoInfo {
        RepoInfo::initial(
            SpecVersionBin::current(),
            a_snapshot_info("initial"),
            100,
            config,
            None,
            &[],
        )
    }

    #[test]
    fn num_updates_per_repo_info_file_reads_the_configured_value()
    -> Result<(), Box<dyn std::error::Error>> {
        for n in [0, 7, 1_000, u16::MAX] {
            let info = with_config(Some(&serde_json::json!({
                "inline_chunk_threshold_bytes": 12,
                "num_updates_per_repo_info_file": n,
            })));
            assert_eq!(info.num_updates_per_repo_info_file()?, Some(n));
        }
        Ok(())
    }

    #[test]
    fn num_updates_per_repo_info_file_is_none_when_unset()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(with_config(None).num_updates_per_repo_info_file()?, None);
        let empty = serde_json::json!({});
        assert_eq!(with_config(Some(&empty)).num_updates_per_repo_info_file()?, None);
        let other_keys = serde_json::json!({"inline_chunk_threshold_bytes": 12});
        assert_eq!(
            with_config(Some(&other_keys)).num_updates_per_repo_info_file()?,
            None
        );
        // how serde writes an unset `Option` field
        let null = serde_json::json!({"num_updates_per_repo_info_file": null});
        assert_eq!(with_config(Some(&null)).num_updates_per_repo_info_file()?, None);
        Ok(())
    }

    #[test]
    fn num_updates_per_repo_info_file_ignores_keys_it_does_not_read()
    -> Result<(), Box<dyn std::error::Error>> {
        // values no `RepositoryConfig` decodes, next to the one key we read
        let config = serde_json::json!({
            "caching": "not a caching config",
            "compression": [1, 2, 3],
            "a_key_from_the_future": {"nested": true},
            "num_updates_per_repo_info_file": 5,
        });
        assert_eq!(with_config(Some(&config)).num_updates_per_repo_info_file()?, Some(5));
        Ok(())
    }

    #[test]
    fn num_updates_per_repo_info_file_it_cannot_decode_reads_as_unset() {
        for value in [
            serde_json::json!("1000"),
            serde_json::json!(-1),
            serde_json::json!(65_536),
            serde_json::json!(1.5),
            serde_json::json!(true),
            serde_json::json!([1]),
            serde_json::json!({"n": 1}),
        ] {
            let config = serde_json::json!({ "num_updates_per_repo_info_file": value });
            let res = with_config(Some(&config)).num_updates_per_repo_info_file();
            assert!(matches!(res, Ok(None)), "{value}: {res:?}");
        }
    }

    #[test]
    fn a_configuration_that_is_not_a_map_reads_as_unset() {
        let res = with_config(Some(&serde_json::json!([1, 2])))
            .num_updates_per_repo_info_file();
        assert!(matches!(res, Ok(None)), "{res:?}");
    }
}
