use err_into::ErrorInto;
use itertools::Itertools as _;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
};

use crate::{format::snapshot::SnapshotProperties, refs::Ref};

use super::{
    IcechunkFormatError, IcechunkFormatErrorKind, IcechunkResult, SnapshotId,
    flatbuffers::generated, format_constants::SpecVersionBin, snapshot::SnapshotInfo,
};

use chrono::{DateTime, Utc};
use flatbuffers::{VerifierOptions, WIPOffset};

// TODO: should we not implement serialize and let the session fetch the repo info?
#[derive(PartialEq, Serialize, Deserialize)]
pub struct RepoInfo {
    buffer: Vec<u8>,
}

// FIXME: make configurable
const UPDATES_PER_FILE: usize = 100;

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
    RepoMigratedUpdate { from_version: SpecVersionBin, to_version: SpecVersionBin },
    ConfigChangedUpdate, // FIXME: implement
    MetadataChangedUpdate,
    TagCreatedUpdate { name: String },
    TagDeletedUpdate { name: String, previous_snap_id: SnapshotId },
    BranchCreatedUpdate { name: String },
    BranchDeletedUpdate { name: String, previous_snap_id: SnapshotId },
    BranchResetUpdate { name: String, previous_snap_id: SnapshotId },
    NewCommitUpdate { branch: String },
    CommitAmendedUpdate { branch: String, previous_snap_id: SnapshotId },
    NewDetachedSnapshotUpdate { new_snap_id: SnapshotId },
    GCRanUpdate,
    ExpirationRanUpdate,
}

static ROOT_OPTIONS: VerifierOptions = VerifierOptions {
    max_depth: 10,
    max_tables: 500_000,
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
    pub fn new<
        'a,
        I: IntoIterator<Item = IcechunkResult<(UpdateType, DateTime<Utc>, Option<&'a str>)>>,
    >(
        tags: impl IntoIterator<Item = (&'a str, SnapshotId)>,
        branches: impl IntoIterator<Item = (&'a str, SnapshotId)>,
        deleted_tags: impl IntoIterator<Item = &'a str>,
        snapshots: impl IntoIterator<Item = SnapshotInfo>,
        metadata: &SnapshotProperties,
        update: UpdateInfo<I>,
        backup_path: Option<&'a str>,
    ) -> IcechunkResult<Self> {
        let mut snapshots: Vec<_> = snapshots.into_iter().collect();
        snapshots.sort_by(|a, b| a.id.0.cmp(&b.id.0));
        let tags = resolve_ref_iter(&snapshots, tags)?;
        let branches = resolve_ref_iter(&snapshots, branches)?;
        let mut deleted_tags: Vec<_> = deleted_tags.into_iter().collect();
        deleted_tags.sort();
        Self::from_parts(
            tags,
            branches,
            deleted_tags,
            snapshots,
            metadata,
            update,
            backup_path,
        )
    }

    fn from_parts<
        'a,
        I: IntoIterator<Item = IcechunkResult<(UpdateType, DateTime<Utc>, Option<&'a str>)>>,
    >(
        sorted_tags: impl IntoIterator<Item = (&'a str, u32)>,
        sorted_branches: impl IntoIterator<Item = (&'a str, u32)>,
        sorted_deleted_tags: impl IntoIterator<Item = &'a str>,
        sorted_snapshots: impl IntoIterator<Item = SnapshotInfo>,
        metadata: &SnapshotProperties,
        update: UpdateInfo<I>,
        backup_path: Option<&'a str>,
    ) -> IcechunkResult<Self> {
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
        // FIXME: shouldn't be assert
        assert!(main_found);
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
                        let index = snapshot_index.get(parent_id).ok_or(
                            IcechunkFormatError::from(
                                IcechunkFormatErrorKind::SnapshotIdNotFound {
                                    snapshot_id: snap.id.clone(),
                                },
                            ),
                        )?;
                        Ok(*index as i32)
                    }
                    None => Ok::<_, IcechunkFormatError>(-1),
                }?;

                let metadata_items: Vec<_> = snap
                    .metadata
                    .iter()
                    .map(|(k, v)| {
                        let name = builder.create_shared_string(k.as_str());
                        let serialized = flexbuffers::to_vec(v).map_err(Box::new)?;
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

        let status = generated::RepoStatus::create(
            &mut builder,
            &generated::RepoStatusArgs {
                availability: generated::RepoAvailability::Online, // TODO:
                set_at: 0,
                limited_availability_reason: None,
            },
        );

        let metadata_items: Vec<_> = metadata
            .iter()
            .map(|(k, v)| {
                let name = builder.create_shared_string(k.as_str());
                let serialized = flexbuffers::to_vec(v).map_err(Box::new)?;
                let value = builder.create_vector(serialized.as_slice());
                let item = generated::MetadataItem::create(
                    &mut builder,
                    &generated::MetadataItemArgs { name: Some(name), value: Some(value) },
                );
                Ok::<_, IcechunkFormatError>(item)
            })
            .try_collect()?;

        let metadata = builder.create_vector(metadata_items.as_slice());

        let (latest_updates, repo_before_updates) =
            Self::mk_latest_updates(&mut builder, update, backup_path)?;

        // TODO: provide accessors for last_updated_at, status, metadata, etc.
        let repo_args = generated::RepoArgs {
            tags: Some(tags),
            branches: Some(branches),
            deleted_tags: Some(deleted_tags),
            snapshots: Some(snapshots),
            spec_version: SpecVersionBin::current() as u8,
            status: Some(status),
            metadata: Some(metadata),
            latest_updates: Some(latest_updates),
            repo_before_updates,
        };
        let repo = generated::Repo::create(&mut builder, &repo_args);
        builder.finish(repo, Some("Ichk"));
        let (mut buffer, offset) = builder.collapse();
        buffer.drain(0..offset);
        buffer.shrink_to_fit();
        Ok(Self { buffer })
    }

    #[allow(clippy::type_complexity)]
    fn mk_latest_updates<
        'bldr,
        'a,
        I: IntoIterator<Item = IcechunkResult<(UpdateType, DateTime<Utc>, Option<&'a str>)>>,
    >(
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
        update: UpdateInfo<I>,
        backup_path: Option<&'a str>,
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
        // assert backup path is only present when there are previous updates
        assert!(
            last_update.is_none() && backup_path.is_none()
                || last_update.is_some() && backup_path.is_some(),
            "A backup path must be provided if and only if there are previous updates"
        );

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
        let mut repo_before_updates = None;

        let updates: Vec<_> = all_updates
            .take(UPDATES_PER_FILE + 1)
            .enumerate()
            .map(|(idx, maybe_data)| maybe_data.map(|(d1, d2, d3)| (idx, d1, d2, d3)))
            .map(|maybe_data| {
                let (idx, u_type, u_time, file) = maybe_data?;
                if idx == UPDATES_PER_FILE {
                    repo_before_updates = file;
                }
                let (update_type_type, update_type) =
                    update_type_to_fb(builder, &u_type)?;
                let file = file.map(|file| builder.create_string(file));
                let res = generated::Update::create(
                    builder,
                    &generated::UpdateArgs {
                        update_type_type,
                        update_type: Some(update_type),
                        updated_at: u_time.timestamp_micros() as u64,
                        backup_path: file,
                    },
                );
                Ok::<_, IcechunkFormatError>(res)
            })
            .try_collect()?;

        debug_assert!(
            updates.len() <= UPDATES_PER_FILE + 1,
            "Too many latest updates in repo file"
        );

        let size = (UPDATES_PER_FILE - 1).min(updates.len() - 1);
        let updates = builder.create_vector(&updates[0..=size]);
        let repo_before_updates = repo_before_updates.map(|s| builder.create_string(s));
        Ok((updates, repo_before_updates))
    }

    pub fn initial(snapshot: SnapshotInfo) -> Self {
        let last_updated_at = snapshot.flushed_at;
        #[allow(clippy::expect_used)]
        // This method is basically constant, so it's OK to unwrap in it
        Self::from_parts(
            [],
            [("main", 0)],
            [],
            [snapshot],
            &Default::default(),
            UpdateInfo {
                update_type: UpdateType::RepoInitializedUpdate,
                update_time: last_updated_at,
                previous_updates: [],
            },
            None,
        )
        .expect("Cannot generate initial snapshot")
    }

    pub fn metadata(&self) -> IcechunkResult<SnapshotProperties> {
        self.root()?
            .metadata()
            .unwrap_or_default()
            .iter()
            .map(|item| {
                let key = item.name().to_string();
                let value =
                    flexbuffers::from_slice(item.value().bytes()).map_err(Box::new)?;
                Ok((key, value))
            })
            .try_collect()
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

    pub fn add_snapshot(
        &self,
        snap: SnapshotInfo,
        branch: Option<&str>,
        update_type: UpdateType,
        previous_file: &str,
    ) -> IcechunkResult<Self> {
        let flushed_at = snap.flushed_at;
        let mut snapshots: Vec<_> = self.all_snapshots()?.try_collect()?;
        let new_index = match snapshots.binary_search_by_key(&&snap.id, |snap| &snap.id) {
            Ok(_) => Err(IcechunkFormatError::from(
                IcechunkFormatErrorKind::DuplicateSnapshotId {
                    snapshot_id: snap.id.clone(),
                },
            )),
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
            tags,
            branches,
            self.deleted_tags()?,
            snapshots,
            &self.metadata()?,
            UpdateInfo {
                update_type,
                update_time: flushed_at,
                previous_updates: self.latest_updates()?,
            },
            Some(previous_file),
        )?;
        Ok(res)
    }

    pub fn add_branch(
        &self,
        name: &str,
        snap: &SnapshotId,
        previous_file: &str,
    ) -> IcechunkResult<Self> {
        if let Ok(snapshot_id) = self.resolve_branch(name) {
            return Err(IcechunkFormatErrorKind::BranchAlreadyExists {
                branch: name.to_string(),
                snapshot_id,
            }
            .into());
        }

        match self.resolve_snapshot_index(snap)? {
            Some(snap_idx) => {
                let mut branches: Vec<_> = self.all_branches()?.collect();
                branches.push((name, snap_idx as u32));
                branches.sort_by(|(name1, _), (name2, _)| name1.cmp(name2));
                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                Ok(Self::from_parts(
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
                )?)
            }
            None => Err(IcechunkFormatErrorKind::SnapshotIdNotFound {
                snapshot_id: snap.clone(),
            }
            .into()),
        }
    }

    pub fn delete_branch(&self, name: &str, previous_file: &str) -> IcechunkResult<Self> {
        match self.resolve_branch(name) {
            Ok(previous_snap_id) => {
                let mut branches: Vec<_> = self.all_branches()?.collect();
                // retain preserves order
                branches.retain(|(n, _)| n != &name);
                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                Self::from_parts(
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
                )
            }
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::BranchNotFound { .. },
                ..
            }) => {
                Err(IcechunkFormatErrorKind::BranchNotFound { branch: name.to_string() }
                    .into())
            }
            Err(err) => Err(err),
        }
    }

    pub fn update_branch(
        &self,
        name: &str,
        new_snap: &SnapshotId,
        previous_file: &str,
    ) -> IcechunkResult<Self> {
        let previous_snap_id = self.resolve_branch(name)?;
        match self.resolve_snapshot_index(new_snap)? {
            Some(snap_idx) => {
                let branches = self.all_branches()?.map(|(br, idx)| {
                    if br == name { (br, snap_idx as u32) } else { (br, idx) }
                });
                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                Ok(Self::from_parts(
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
                )?)
            }
            None => Err(IcechunkFormatErrorKind::SnapshotIdNotFound {
                snapshot_id: new_snap.clone(),
            }
            .into()),
        }
    }

    pub fn add_tag(
        &self,
        name: &str,
        snap: &SnapshotId,
        previous_file: &str,
    ) -> IcechunkResult<Self> {
        if self.resolve_tag(name).is_ok() || self.tag_was_deleted(name)? {
            // TODO: better error on tag already deleted
            return Err(IcechunkFormatErrorKind::TagAlreadyExists {
                tag: name.to_string(),
            }
            .into());
        }

        match self.resolve_snapshot_index(snap)? {
            Some(snap_idx) => {
                let mut tags: Vec<_> = self.all_tags()?.collect();
                tags.push((name, snap_idx as u32));
                tags.sort_by(|(name1, _), (name2, _)| name1.cmp(name2));
                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                Ok(Self::from_parts(
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
                )?)
            }
            None => Err(IcechunkFormatErrorKind::SnapshotIdNotFound {
                snapshot_id: snap.clone(),
            }
            .into()),
        }
    }

    pub fn delete_tag(&self, name: &str, previous_file: &str) -> IcechunkResult<Self> {
        match self.resolve_tag(name) {
            Ok(previous_snap_id) => {
                let mut tags: Vec<_> = self.all_tags()?.collect();
                // retain preserves order
                tags.retain(|(n, _)| n != &name);

                let mut deleted_tags: BTreeSet<_> = self.deleted_tags()?.collect();
                deleted_tags.insert(name);

                let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
                Self::from_parts(
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
                )
            }
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::TagNotFound { .. },
                ..
            }) => {
                Err(IcechunkFormatErrorKind::TagNotFound { tag: name.to_string() }.into())
            }
            Err(err) => Err(err),
        }
    }

    pub fn set_metadata(
        &self,
        metadata: &SnapshotProperties,
        previous_file: &str,
    ) -> IcechunkResult<Self> {
        let snaps: Vec<_> = self.all_snapshots()?.try_collect()?;
        Self::from_parts(
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
        )
    }

    pub fn from_buffer(buffer: Vec<u8>) -> IcechunkResult<RepoInfo> {
        let _ = flatbuffers::root_with_opts::<generated::Repo>(
            &ROOT_OPTIONS,
            buffer.as_slice(),
        )?;
        Ok(RepoInfo { buffer })
    }

    pub fn bytes(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    fn root(&self) -> IcechunkResult<generated::Repo<'_>> {
        Ok(flatbuffers::root::<generated::Repo>(&self.buffer)?)
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
            .ok_or(IcechunkFormatErrorKind::TagNotFound { tag: name.to_string() })?;

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
            .ok_or(IcechunkFormatErrorKind::BranchNotFound {
                branch: name.to_string(),
            })?;

        Ok(res)
    }

    pub fn spec_version(&self) -> IcechunkResult<SpecVersionBin> {
        self.root()?
            .spec_version()
            .try_into()
            .map_err(|_| IcechunkFormatErrorKind::InvalidSpecVersion)
            .err_into()
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
        #[allow(clippy::unwrap_used)]
        match update.update_type_type() {
            generated::UpdateType::RepoInitializedUpdate => {
                Ok(UpdateType::RepoInitializedUpdate)
            }
            generated::UpdateType::RepoMigratedUpdate => {
                let up = update.update_type_as_repo_migrated_update().unwrap();
                Ok(UpdateType::RepoMigratedUpdate {
                    from_version: up.from_version().try_into().map_err(|_| {
                        IcechunkFormatError::from(
                            IcechunkFormatErrorKind::InvalidSpecVersion,
                        )
                    })?,
                    to_version: up.to_version().try_into().map_err(|_| {
                        IcechunkFormatError::from(
                            IcechunkFormatErrorKind::InvalidSpecVersion,
                        )
                    })?,
                })
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
                Ok(UpdateType::NewCommitUpdate { branch: up.branch().to_string() })
            }
            generated::UpdateType::CommitAmendedUpdate => {
                let up = update.update_type_as_commit_amended_update().unwrap();
                let previous_snap_id = SnapshotId::new(up.previous_snap_id().0);
                Ok(UpdateType::CommitAmendedUpdate {
                    branch: up.branch().to_string(),
                    previous_snap_id,
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
            _ => Err(IcechunkFormatErrorKind::InvalidFlatBuffer(
                flatbuffers::InvalidFlatbuffer::InconsistentUnion {
                    field: Cow::Borrowed("latest_update_type"),
                    field_type: Cow::Borrowed("UpdateType"),
                    error_trace: Default::default(),
                },
            )
            .into()),
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
            }
            .into())
        }
    }

    pub fn find_snapshot(&self, id: &SnapshotId) -> IcechunkResult<SnapshotInfo> {
        let mut anc = self.ancestry(id)?;
        #[allow(clippy::panic)]
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
        // TODO: replace by binary search
        Ok(self.root()?.snapshots().iter().position(|snap| snap.id().0 == id.0))
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
                .map_err(|_| {
                    IcechunkFormatError::from(
                        IcechunkFormatErrorKind::SnapshotIdNotFound {
                            snapshot_id: id.clone(),
                        },
                    )
                })? as u32;
            Ok::<_, IcechunkFormatError>((name, idx))
        })
        .try_collect()?;
    res.sort_by(|(name1, _), (name2, _)| name1.cmp(name2));
    Ok(res)
}

fn timestamp_to_timestamp(ts: u64) -> IcechunkResult<DateTime<Utc>> {
    let ts: i64 = ts.try_into().map_err(|_| {
        IcechunkFormatError::from(IcechunkFormatErrorKind::InvalidTimestamp)
    })?;
    DateTime::from_timestamp_micros(ts)
        .ok_or_else(|| IcechunkFormatErrorKind::InvalidTimestamp.into())
}

fn mk_snapshot_info(
    repo: &generated::Repo,
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
                        .map_err(Box::new)?;
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
) -> IcechunkResult<(
    generated::UpdateType,
    flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>,
)> {
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
        UpdateType::NewCommitUpdate { branch } => {
            let branch = Some(builder.create_string(branch));
            Ok((
                generated::UpdateType::NewCommitUpdate,
                generated::NewCommitUpdate::create(
                    builder,
                    &generated::NewCommitUpdateArgs { branch },
                )
                .as_union_value(),
            ))
        }
        UpdateType::CommitAmendedUpdate { branch, previous_snap_id } => {
            let branch = Some(builder.create_string(branch));
            let object_id12 = generated::ObjectId12::new(&previous_snap_id.0);
            let previous_snap_id = Some(&object_id12);
            Ok((
                generated::UpdateType::CommitAmendedUpdate,
                generated::CommitAmendedUpdate::create(
                    builder,
                    &generated::CommitAmendedUpdateArgs { branch, previous_snap_id },
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
    }
}

#[cfg(test)]
#[allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
mod tests {

    use super::*;
    use std::collections::HashSet;

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
        let repo = RepoInfo::initial(snap1.clone());
        assert_eq!(repo.all_snapshots()?.next().unwrap().unwrap(), snap1);

        let id2 = SnapshotId::random();
        let snap2 = SnapshotInfo {
            id: id2.clone(),
            parent_id: Some(id1.clone()),
            message: "snap 2".to_string(),
            ..snap1.clone()
        };
        let repo = repo.add_snapshot(
            snap2.clone(),
            Some("main"),
            UpdateType::NewCommitUpdate { branch: "main".to_string() },
            "foo/bar",
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
            message: "snap 3".to_string(),
            ..snap2.clone()
        };
        let repo = repo.add_snapshot(
            snap3.clone(),
            Some("main"),
            UpdateType::NewCommitUpdate { branch: "main".to_string() },
            "foo",
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
        let repo = RepoInfo::initial(snap1.clone());
        let repo = repo.add_branch("foo", &id1, "foo")?;
        let repo = repo.add_branch("bar", &id1, "bar")?;
        assert!(matches!(
            repo.add_branch("bad-snap", &SnapshotId::random(), "bad"),
            Err(IcechunkFormatError {
                kind: IcechunkFormatErrorKind::SnapshotIdNotFound { .. },
                ..
            })
        ));
        // cannot add existing
        assert!(matches!(
            repo.add_branch("bar", &id1, "/foo/bar"),
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
            message: "snap 2".to_string(),
            ..snap1.clone()
        };
        let repo = repo.add_snapshot(
            snap2,
            Some("main"),
            UpdateType::NewCommitUpdate { branch: "main".to_string() },
            "foo",
        )?;
        let repo = repo.add_branch("baz", &id2, "/foo/bar")?;
        assert_eq!(repo.resolve_branch("main")?, id2.clone());
        assert_eq!(repo.resolve_branch("foo")?, id1.clone());
        assert_eq!(repo.resolve_branch("bar")?, id1.clone());
        assert_eq!(repo.resolve_branch("baz")?, id2.clone());

        let repo = repo.delete_branch("bar", "bar")?;
        assert!(repo.resolve_branch("bar").is_err());
        assert_eq!(
            repo.all_branches()?.map(|(n, _)| n).collect::<HashSet<_>>(),
            ["main", "foo", "baz"].into()
        );

        assert!(repo.delete_branch("bad-branch", "bad").is_err());

        // tags
        let repo = repo.add_tag("tag1", &id1, "tag1")?;
        let repo = repo.add_tag("tag2", &id2, "tag2")?;
        assert!(repo.add_tag("bad-snap", &SnapshotId::random(), "bad").is_err());
        assert!(repo.add_tag("tag1", &id1, "tag1-again").is_err());
        assert_eq!(repo.resolve_tag("tag1")?, id1.clone());
        assert_eq!(repo.resolve_tag("tag2")?, id2.clone());
        assert_eq!(
            repo.all_tags()?.map(|(n, _)| n).collect::<HashSet<_>>(),
            ["tag1", "tag2"].into()
        );

        // delete tags
        let repo = repo.add_tag("tag3", &id1, "tag3")?;
        let repo = repo.delete_tag("tag3", "delete-tag3")?;
        assert_eq!(
            repo.all_tags()?.map(|(n, _)| n).collect::<HashSet<_>>(),
            ["tag1", "tag2"].into()
        );
        // cannot add deleted
        assert!(repo.add_tag("tag3", &id1, "tag3-again").is_err());
        // cannot delete deleted
        assert!(repo.delete_tag("tag3", "delete-tag3-again").is_err());
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

        // check updates for a new repo
        let mut repo = RepoInfo::initial(snap1);
        assert_eq!(repo.latest_updates()?.count(), 1);
        let (last_update, _, file) = repo.latest_updates()?.next().unwrap()?;
        assert!(file.is_none());
        assert_eq!(last_update, UpdateType::RepoInitializedUpdate);
        assert!(repo.repo_before_updates()?.is_none());

        // check updates after UPDATES_PER_FILE changes
        // fill the first page of updates by adding branches
        for i in 1..=(UPDATES_PER_FILE - 1) {
            repo = repo.add_branch(
                i.to_string().as_str(),
                &id1,
                (i - 1).to_string().as_str(),
            )?
        }

        assert_eq!(repo.latest_updates()?.count(), UPDATES_PER_FILE);
        let updates = repo.latest_updates()?;

        // check all other updates
        for (idx, update) in updates.enumerate() {
            let (update, _, file) = update?;
            if idx == UPDATES_PER_FILE - 1 {
                assert_eq!(update, UpdateType::RepoInitializedUpdate);
                assert_eq!(file, Some("0"));
            } else {
                assert_eq!(
                    update,
                    UpdateType::BranchCreatedUpdate {
                        name: (UPDATES_PER_FILE - 1 - idx).to_string()
                    }
                );
                if idx == 0 {
                    assert!(file.is_none())
                } else {
                    assert_eq!(
                        file,
                        Some((UPDATES_PER_FILE - 1 - idx).to_string().as_str())
                    );
                }
            }
        }
        assert!(repo.repo_before_updates()?.is_none());

        // Now, if we add another change, it won't fit in the first "page" of repo updates
        repo = repo.add_tag("tag", &id1, "first-branches")?;
        // the file only contains the first "page" worth of updates
        assert_eq!(repo.latest_updates()?.count(), UPDATES_PER_FILE);
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
            UpdateType::BranchCreatedUpdate { name: "99".to_string() }
        );
        assert_eq!(file, Some("first-branches"));

        // all other changes are branch creation (repo creation is in the next page)
        for (idx, update) in updates.enumerate() {
            let (update, _, file) = update?;
            assert_eq!(file, Some((UPDATES_PER_FILE - 2 - idx).to_string().as_str()));
            assert_eq!(
                update,
                UpdateType::BranchCreatedUpdate {
                    name: (UPDATES_PER_FILE - 2 - idx).to_string()
                }
            );
        }

        Ok(())
    }
}
