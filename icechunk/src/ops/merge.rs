//! Merge snapshots into one commit on a branch.
//!
//! A source is any snapshot whose parent is an ancestor of the branch tip,
//! usually a detached snapshot written by `Session::flush`. The merge checks
//! the source transaction logs against each other and against the commits
//! between their parents and the tip, then writes one snapshot that applies
//! every source on top of the tip.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use futures::{StreamExt as _, TryStreamExt as _, stream};
use tracing::{debug, info, instrument};

use crate::{
    asset_manager::AssetManager,
    change_set::ChunkTable,
    config::RepositoryConfig,
    conflicts::Conflict,
    format::{
        ChunkIndices, IcechunkFormatError, IcechunkFormatErrorKind, ManifestId, NodeId,
        Path, SnapshotId,
        manifest::{ManifestExtents, ManifestRef, Overlap},
        repo_info::RepoInfo,
        snapshot::{
            ManifestFileInfo, NodeData, NodeSnapshot, Snapshot, SnapshotInfo,
            SnapshotProperties,
        },
        transaction_log::TransactionLog,
    },
    repository::{RepositoryError, RepositoryErrorKind},
    session::{
        CommitMethod, SessionError, SessionErrorKind, SessionResult, do_commit_v2,
        fetch_manifest, write_manifest_with_changes,
    },
    storage::StorageErrorKind,
};
use icechunk_types::{
    ICResultExt as _,
    error::{ICError, ICResultCtxExt as _},
};

/// A conflict between two snapshots found during a merge.
///
/// `first_snapshot` is the log treated as already applied, `second_snapshot`
/// the log applied on top of it. See `detect_conflicts`.
#[derive(Debug, PartialEq, Eq)]
pub struct MergeConflict {
    pub first_snapshot: SnapshotId,
    pub second_snapshot: SnapshotId,
    pub conflict: Conflict,
}

/// One source and the commits it must be checked against.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SourcePlan {
    pub(crate) id: SnapshotId,
    pub(crate) parent: SnapshotId,
    /// Commits strictly between `parent` and the tip, oldest first.
    pub(crate) intervening: Vec<SnapshotInfo>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MergePlan {
    pub(crate) tip: SnapshotId,
    pub(crate) sources: Vec<SourcePlan>,
}

fn not_in_history(snapshot: &SnapshotId, branch: &str) -> SessionError {
    SessionError::capture(SessionErrorKind::SnapshotNotInBranchHistory {
        snapshot: snapshot.clone(),
        branch: branch.to_string(),
    })
}

/// Resolve the branch tip and, for each source, the commits between its
/// parent and the tip. The ancestry walk stops as soon as every parent is found.
pub(crate) fn plan_merge(
    repo_info: &RepoInfo,
    branch: &str,
    snapshots: &[SnapshotId],
) -> SessionResult<MergePlan> {
    if snapshots.is_empty() {
        return Err(SessionError::capture(SessionErrorKind::NoSnapshotsToMerge));
    }
    let tip = repo_info.resolve_branch(branch).inject()?;
    // `ancestry` yields the tip itself first, then its parents.
    let mut ancestry = repo_info.ancestry(&tip).inject()?;
    let mut history: Vec<SnapshotInfo> = Vec::new();
    let mut sources = Vec::with_capacity(snapshots.len());
    for id in snapshots {
        let parent = repo_info
            .find_snapshot(id)
            .inject()?
            .parent_id
            .ok_or_else(|| not_in_history(id, branch))?;
        let parent_index = loop {
            if let Some(index) = history.iter().position(|info| info.id == parent) {
                break index;
            }
            match ancestry.next() {
                Some(info) => history.push(info.inject()?),
                None => return Err(not_in_history(id, branch)),
            }
        };
        // history[0] is the tip, so the commits after the parent are history[..parent_index], newest first.
        let intervening = history[..parent_index].iter().rev().cloned().collect();
        sources.push(SourcePlan { id: id.clone(), parent, intervening });
    }
    Ok(MergePlan { tip, sources })
}

/// Node ids to paths, for conflict reports and new node checks.
pub(crate) struct PathIndex(HashMap<NodeId, Path>);

impl PathIndex {
    /// Index the nodes of several snapshots. The first snapshot that has a node wins.
    pub(crate) fn from_snapshots<'a>(
        snapshots: impl IntoIterator<Item = &'a Snapshot>,
    ) -> SessionResult<Self> {
        let mut index = HashMap::new();
        for snapshot in snapshots {
            for node in snapshot.iter() {
                let node = node.inject()?;
                index.entry(node.id).or_insert(node.path);
            }
        }
        Ok(Self(index))
    }

    #[cfg(test)]
    pub(crate) fn from_pairs(pairs: impl IntoIterator<Item = (NodeId, Path)>) -> Self {
        Self(pairs.into_iter().collect())
    }

    fn get(&self, id: &NodeId) -> Option<&Path> {
        self.0.get(id)
    }

    fn path(&self, id: &NodeId) -> SessionResult<Path> {
        self.get(id).cloned().ok_or_else(|| {
            SessionError::capture(SessionErrorKind::ConflictingPathNotFound(id.clone()))
        })
    }
}

/// A transaction log and the id of the snapshot that wrote it.
pub(crate) struct LogView<'a> {
    pub(crate) id: &'a SnapshotId,
    pub(crate) log: &'a TransactionLog,
}

/// Conflicts between two logs, in the direction the rebase detector uses:
/// `previous` is already applied, `current` is applied on top of it.
pub(crate) fn detect_conflicts(
    previous: &LogView<'_>,
    current: &LogView<'_>,
    paths: &PathIndex,
) -> SessionResult<Vec<MergeConflict>> {
    let mut found = Vec::new();
    let mut push = |conflict: Conflict| {
        found.push(MergeConflict {
            first_snapshot: previous.id.clone(),
            second_snapshot: current.id.clone(),
            conflict,
        });
    };

    // Nodes `previous` created, with whether each one is an array.
    let previous_new: Vec<(&Path, bool)> = previous
        .log
        .new_groups()
        .map(|id| (id, false))
        .chain(previous.log.new_arrays().map(|id| (id, true)))
        .filter_map(|(id, is_array)| paths.get(&id).map(|path| (path, is_array)))
        .collect();
    for id in current.log.new_groups().chain(current.log.new_arrays()) {
        let Some(path) = paths.get(&id) else { continue };
        for (previous_path, is_array) in &previous_new {
            if *previous_path == path {
                push(Conflict::NewNodeConflictsWithExistingNode(path.clone()));
            } else if *is_array
                && path.ancestors().skip(1).any(|ancestor| ancestor == **previous_path)
            {
                push(Conflict::NewNodeInInvalidGroup((*previous_path).clone()));
            }
        }
    }

    for id in current.log.updated_arrays() {
        if previous.log.array_updated(&id) {
            push(Conflict::ZarrMetadataDoubleUpdate(paths.path(&id)?));
        }
        if previous.log.array_deleted(&id) {
            push(Conflict::ZarrMetadataUpdateOfDeletedArray(paths.path(&id)?));
        }
    }
    for id in current.log.updated_groups() {
        if previous.log.group_updated(&id) {
            push(Conflict::ZarrMetadataDoubleUpdate(paths.path(&id)?));
        }
        if previous.log.group_deleted(&id) {
            push(Conflict::ZarrMetadataUpdateOfDeletedGroup(paths.path(&id)?));
        }
    }

    for (id, coords) in current.log.updated_chunks() {
        if previous.log.array_deleted(&id) {
            push(Conflict::ChunksUpdatedInDeletedArray {
                path: paths.path(&id)?,
                node_id: id.clone(),
            });
            continue;
        }
        if previous.log.array_updated(&id) {
            push(Conflict::ChunksUpdatedInUpdatedArray {
                path: paths.path(&id)?,
                node_id: id.clone(),
            });
        }
        if previous.log.chunks_updated(&id) {
            // Only a node both logs wrote to holds a coordinate set in memory.
            let previous_coords: HashSet<ChunkIndices> =
                previous.log.updated_chunks_for(&id).collect();
            let common: HashSet<ChunkIndices> =
                coords.filter(|coord| previous_coords.contains(coord)).collect();
            if !common.is_empty() {
                push(Conflict::ChunkDoubleUpdate {
                    path: paths.path(&id)?,
                    node_id: id.clone(),
                    chunk_coordinates: common,
                });
            }
        }
    }

    for id in current.log.deleted_arrays() {
        if previous.log.array_updated(&id) || previous.log.chunks_updated(&id) {
            push(Conflict::DeleteOfUpdatedArray { path: paths.path(&id)?, node_id: id });
        }
    }
    for id in current.log.deleted_groups() {
        if previous.log.group_updated(&id) {
            push(Conflict::DeleteOfUpdatedGroup { path: paths.path(&id)?, node_id: id });
        }
    }

    Ok(found)
}

/// A source snapshot with everything the merge reads from it.
pub(crate) struct Source {
    pub(crate) id: SnapshotId,
    pub(crate) parent: SnapshotId,
    pub(crate) snapshot: Arc<Snapshot>,
    pub(crate) log: Arc<TransactionLog>,
}

/// The result node list before manifests of existing arrays are merged.
pub(crate) struct MergedNodes {
    /// Result nodes keyed by path string, the order `Snapshot::from_iter` needs.
    /// Arrays that exist in the tip keep the tip's manifest refs.
    pub(crate) nodes: BTreeMap<String, NodeSnapshot>,
    /// Manifest files of arrays new in a source, taken from that source.
    pub(crate) files: HashMap<ManifestId, ManifestFileInfo>,
}

fn manifest_info(
    snapshot: &Snapshot,
    id: &ManifestId,
) -> SessionResult<ManifestFileInfo> {
    snapshot
        .manifest_info(id)
        .inject()?
        .ok_or_else(|| IcechunkFormatErrorKind::ManifestInfoNotFound {
            manifest_id: id.clone(),
        })
        .capture::<IcechunkFormatErrorKind>()
        .inject()
}

/// Apply the node changes of every source to the tip's node list.
///
/// Paths in a source equal paths in the tip because logs with moves are
/// rejected before this runs.
pub(crate) fn merge_nodes(
    tip: &Snapshot,
    sources: &[Source],
) -> SessionResult<MergedNodes> {
    let mut nodes: BTreeMap<String, NodeSnapshot> = BTreeMap::new();
    let mut key_of: HashMap<NodeId, String> = HashMap::new();
    for node in tip.iter() {
        let node = node.inject()?;
        let key = node.path.to_string();
        key_of.insert(node.id.clone(), key.clone());
        nodes.insert(key, node);
    }
    let mut files = HashMap::new();
    for source in sources {
        for id in source.log.deleted_groups().chain(source.log.deleted_arrays()) {
            if let Some(key) = key_of.remove(&id) {
                nodes.remove(&key);
            }
        }
        let new_ids: HashSet<NodeId> =
            source.log.new_groups().chain(source.log.new_arrays()).collect();
        let mut wanted: HashSet<NodeId> =
            source.log.updated_groups().chain(source.log.updated_arrays()).collect();
        wanted.extend(new_ids.iter().cloned());
        for node in source.snapshot.iter() {
            let node = node.inject()?;
            if !wanted.contains(&node.id) {
                continue;
            }
            let key = node.path.to_string();
            let NodeSnapshot { id, path, user_data, node_data } = node;
            let node_data = if new_ids.contains(&id) {
                if let NodeData::Array { manifests, .. } = &node_data {
                    for mref in manifests {
                        files.insert(
                            mref.object_id.clone(),
                            manifest_info(source.snapshot.as_ref(), &mref.object_id)?,
                        );
                    }
                }
                node_data
            } else {
                // A metadata update keeps the tip's manifests, which Step 5 merges.
                match (node_data, nodes.get(&key).map(|n| &n.node_data)) {
                    (
                        NodeData::Array { shape, dimension_names, .. },
                        Some(NodeData::Array { manifests, .. }),
                    ) => NodeData::Array {
                        shape,
                        dimension_names,
                        manifests: manifests.clone(),
                    },
                    (node_data, _) => node_data,
                }
            };
            key_of.insert(id.clone(), key.clone());
            nodes.insert(key, NodeSnapshot { id, path, user_data, node_data });
        }
    }
    Ok(MergedNodes { nodes, files })
}

/// Merged manifest refs and files for one array that exists in the tip.
pub(crate) struct ArrayManifests {
    pub(crate) refs: Vec<ManifestRef>,
    pub(crate) files: Vec<ManifestFileInfo>,
}

/// Merge the chunk changes of `sources` into the tip's manifests for `node`,
/// one split at a time. Space is proportional to one split plus the
/// coordinates the sources wrote to this node.
pub(crate) async fn merge_array_manifests(
    asset_manager: &AssetManager,
    config: &RepositoryConfig,
    tip: &Snapshot,
    node: &NodeSnapshot,
    sources: &[&Source],
) -> SessionResult<ArrayManifests> {
    let mut result = ArrayManifests { refs: Vec::new(), files: Vec::new() };
    let NodeData::Array { shape, dimension_names, manifests: tip_refs } = &node.node_data
    else {
        return Ok(result);
    };
    let splits =
        config.manifest().splitting().get_split_sizes(&node.path, shape, dimension_names);

    // Coordinates per split, with the index of the source that wrote each one.
    let mut touched: HashMap<ManifestExtents, Vec<(usize, ChunkIndices)>> =
        HashMap::new();
    for (index, source) in sources.iter().enumerate() {
        for coord in source.log.updated_chunks_for(&node.id) {
            if let Some(extent) = splits.find(&coord) {
                touched.entry(extent).or_default().push((index, coord));
            }
        }
    }

    let tip_id = tip.id();
    for extent in splits.iter() {
        let intersecting: Vec<(&ManifestRef, Overlap)> = tip_refs
            .iter()
            .filter_map(|mref| match mref.extents.overlap_with(&extent) {
                Overlap::None => None,
                overlap => Some((mref, overlap)),
            })
            .collect();
        match touched.remove(&extent) {
            Some(coords) => {
                if let Some((mref, file)) =
                    reusable_manifest(&coords, sources, node, &extent, &tip_id)?
                {
                    result.refs.push(mref);
                    result.files.push(file);
                    continue;
                }
                let table =
                    modified_chunks(asset_manager, sources, node, &coords).await?;
                if let Some((new_ref, file)) = write_manifest_with_changes(
                    asset_manager,
                    config.manifest(),
                    intersecting.iter().map(|(mref, _)| *mref),
                    table,
                    &extent,
                    &node.id,
                    &tip_id,
                )
                .await?
                {
                    result.refs.push(new_ref);
                    result.files.push(file);
                }
            }
            None => {
                for (mref, overlap) in intersecting {
                    if overlap == Overlap::Complete {
                        result.refs.push(mref.clone());
                        result.files.push(manifest_info(tip, &mref.object_id)?);
                    } else if let Some((new_ref, file)) = write_manifest_with_changes(
                        asset_manager,
                        config.manifest(),
                        std::iter::once(mref),
                        ChunkTable::default(),
                        &extent,
                        &node.id,
                        &tip_id,
                    )
                    .await?
                    {
                        result.refs.push(new_ref);
                        result.files.push(file);
                    }
                }
            }
        }
    }
    Ok(result)
}

/// The source manifest that already equals the merged split: one source wrote
/// the split, its parent is the tip, and exactly one of its manifests
/// intersects the split, lying fully inside it.
///
/// A manifest's own extents are trimmed to the coordinates it actually holds
/// (see `write_manifest_from_stream`), so they are usually narrower than the
/// split boundary; `Overlap::Complete` is the right test, not equality. A
/// source can also split its manifests more finely than this merge does, so
/// more than one of its manifests can intersect the split: reuse only applies
/// when a single manifest already holds the split's entire content, otherwise
/// the other intersecting manifests' chunks would be silently dropped.
fn reusable_manifest(
    coords: &[(usize, ChunkIndices)],
    sources: &[&Source],
    node: &NodeSnapshot,
    extent: &ManifestExtents,
    tip_id: &SnapshotId,
) -> SessionResult<Option<(ManifestRef, ManifestFileInfo)>> {
    let Some(&(first, _)) = coords.first() else { return Ok(None) };
    if coords.iter().any(|(index, _)| *index != first) {
        return Ok(None);
    }
    let Some(source) = sources.get(first) else { return Ok(None) };
    if &source.parent != tip_id {
        return Ok(None);
    }
    let source_node = source.snapshot.get_node(&node.path).inject()?;
    let NodeData::Array { manifests, .. } = &source_node.node_data else {
        return Ok(None);
    };
    let intersecting: Vec<&ManifestRef> = manifests
        .iter()
        .filter(|mref| mref.extents.overlap_with(extent) != Overlap::None)
        .collect();
    let [mref] = intersecting.as_slice() else { return Ok(None) };
    if mref.extents.overlap_with(extent) != Overlap::Complete {
        return Ok(None);
    }
    let file = manifest_info(source.snapshot.as_ref(), &mref.object_id)?;
    Ok(Some(((*mref).clone(), file)))
}

/// Chunk references the sources wrote for `coords`, read from the source
/// manifests. A coordinate absent from its source manifest is a deletion.
async fn modified_chunks(
    asset_manager: &AssetManager,
    sources: &[&Source],
    node: &NodeSnapshot,
    coords: &[(usize, ChunkIndices)],
) -> SessionResult<ChunkTable> {
    let mut table = ChunkTable::new();
    for (index, coord) in coords {
        let Some(source) = sources.get(*index) else { continue };
        let source_node = source.snapshot.get_node(&node.path).inject()?;
        let NodeData::Array { manifests, .. } = &source_node.node_data else { continue };
        let mut payload = None;
        for mref in manifests.iter().filter(|mref| mref.extents.contains(&coord.0)) {
            let manifest =
                fetch_manifest(&mref.object_id, &source.id, asset_manager).await?;
            match manifest.get_chunk_payload(&node.id, coord) {
                Ok(found) => {
                    payload = Some(found);
                    break;
                }
                Err(IcechunkFormatError {
                    kind: IcechunkFormatErrorKind::ChunkCoordinatesNotFound { .. },
                    ..
                }) => {}
                Err(err) => return Err(err).inject(),
            }
        }
        table.insert(coord.clone(), payload);
    }
    Ok(table)
}

/// Fetch a pruned ancestor log. A missing one would hide conflicts, so it fails the merge.
async fn fetch_pruned_log(
    asset_manager: &AssetManager,
    log_id: &SnapshotId,
    commit: &SnapshotId,
) -> SessionResult<Arc<TransactionLog>> {
    match asset_manager.fetch_transaction_log(log_id).await {
        Ok(log) => Ok(log),
        Err(err)
            if matches!(
                err.kind,
                RepositoryErrorKind::StorageError(StorageErrorKind::ObjectNotFound)
            ) =>
        {
            Err(SessionError::capture(SessionErrorKind::MissingPrunedAncestorTxLog {
                snapshot: commit.clone(),
                tx_log: log_id.clone(),
            }))
        }
        Err(err) => Err(err).inject(),
    }
}

fn move_conflict(id: &SnapshotId) -> MergeConflict {
    MergeConflict {
        first_snapshot: id.clone(),
        second_snapshot: id.clone(),
        conflict: Conflict::MoveOperationCannotBeRebased,
    }
}

/// Merge the sources of an already computed plan and commit on `branch`.
#[instrument(skip(asset_manager, config, plan, properties))]
pub(crate) async fn merge_planned(
    asset_manager: Arc<AssetManager>,
    config: &RepositoryConfig,
    branch: &str,
    plan: MergePlan,
    message: &str,
    properties: SnapshotProperties,
) -> SessionResult<SnapshotId> {
    info!(tip = %plan.tip, sources = plan.sources.len(), "Merge started");
    let tip = asset_manager.fetch_snapshot(&plan.tip).await.inject()?;

    let mut sources = Vec::with_capacity(plan.sources.len());
    for source in &plan.sources {
        sources.push(Source {
            id: source.id.clone(),
            parent: source.parent.clone(),
            snapshot: asset_manager.fetch_snapshot(&source.id).await.inject()?,
            log: asset_manager.fetch_transaction_log(&source.id).await.inject()?,
        });
    }
    let parent_ids: HashSet<&SnapshotId> = sources.iter().map(|s| &s.parent).collect();
    let mut parents = Vec::with_capacity(parent_ids.len());
    for id in parent_ids {
        parents.push(asset_manager.fetch_snapshot(id).await.inject()?);
    }
    let paths = PathIndex::from_snapshots(
        std::iter::once(tip.as_ref())
            .chain(sources.iter().map(|s| s.snapshot.as_ref()))
            .chain(parents.iter().map(|p| p.as_ref())),
    )?;

    let mut conflicts = Vec::new();
    for source in &sources {
        if source.log.has_moves() {
            conflicts.push(move_conflict(&source.id));
        }
    }
    for (planned, source) in plan.sources.iter().zip(&sources) {
        let current = LogView { id: &source.id, log: &source.log };
        for commit in &planned.intervening {
            for log_id in &commit.pruned_ancestor_tx_logs {
                let log = fetch_pruned_log(&asset_manager, log_id, &commit.id).await?;
                if log.has_moves() {
                    conflicts.push(move_conflict(log_id));
                }
                conflicts.extend(detect_conflicts(
                    &LogView { id: log_id, log: &log },
                    &current,
                    &paths,
                )?);
            }
            let log = asset_manager.fetch_transaction_log(&commit.id).await.inject()?;
            if log.has_moves() {
                conflicts.push(move_conflict(&commit.id));
            }
            conflicts.extend(detect_conflicts(
                &LogView { id: &commit.id, log: &log },
                &current,
                &paths,
            )?);
        }
    }
    for (index, earlier) in sources.iter().enumerate() {
        for later in sources.iter().skip(index + 1) {
            conflicts.extend(detect_conflicts(
                &LogView { id: &earlier.id, log: &earlier.log },
                &LogView { id: &later.id, log: &later.log },
                &paths,
            )?);
        }
    }
    if !conflicts.is_empty() {
        debug!(count = conflicts.len(), "Merge aborted, conflicts found");
        return Err(SessionError::capture(SessionErrorKind::MergeConflict { conflicts }));
    }

    let MergedNodes { mut nodes, mut files } = merge_nodes(tip.as_ref(), &sources)?;
    for file in tip.manifest_files() {
        let file = file.inject()?;
        files.insert(file.id.clone(), file);
    }

    // Arrays of the tip that a source wrote chunks to. Arrays a source created
    // already carry that source's manifests.
    let touched: Vec<(String, NodeSnapshot, Vec<&Source>)> = nodes
        .iter()
        .filter_map(|(key, node)| {
            let writers: Vec<&Source> = sources
                .iter()
                .filter(|s| {
                    s.log.chunks_updated(&node.id) && !s.log.array_created(&node.id)
                })
                .collect();
            (!writers.is_empty()).then(|| (key.clone(), node.clone(), writers))
        })
        .collect();
    let max_concurrent =
        usize::from(config.manifest().max_concurrent_manifest_fetches_during_commit())
            .max(1);
    let merged: Vec<(String, ArrayManifests)> = stream::iter(touched)
        .map(|(key, node, writers)| {
            let asset_manager = Arc::clone(&asset_manager);
            let tip = Arc::clone(&tip);
            async move {
                let result = merge_array_manifests(
                    asset_manager.as_ref(),
                    config,
                    tip.as_ref(),
                    &node,
                    &writers,
                )
                .await?;
                Ok::<_, SessionError>((key, result))
            }
        })
        .buffer_unordered(max_concurrent)
        .try_collect()
        .await?;
    for (key, ArrayManifests { refs, files: new_files }) in merged {
        if let Some(NodeSnapshot {
            node_data: NodeData::Array { manifests, .. }, ..
        }) = nodes.get_mut(&key)
        {
            *manifests = refs;
        }
        for file in new_files {
            files.insert(file.id.clone(), file);
        }
    }

    // Every ref in the result must have a file; unreferenced tip files are dropped.
    let mut manifest_files: BTreeMap<ManifestId, ManifestFileInfo> = BTreeMap::new();
    for node in nodes.values() {
        if let NodeData::Array { manifests, .. } = &node.node_data {
            for mref in manifests {
                let file = files
                    .get(&mref.object_id)
                    .cloned()
                    .ok_or_else(|| IcechunkFormatErrorKind::ManifestInfoNotFound {
                        manifest_id: mref.object_id.clone(),
                    })
                    .capture::<IcechunkFormatErrorKind>()
                    .inject()?;
                manifest_files.insert(mref.object_id.clone(), file);
            }
        }
    }

    let new_snapshot = Snapshot::from_iter(
        None,
        None,
        asset_manager.spec_version(),
        message,
        Some(properties),
        manifest_files.into_values().collect(),
        None,
        nodes.into_values().map(Ok),
    )
    .inject()?;
    let new_ts = new_snapshot.flushed_at().inject()?;
    let old_ts = tip.flushed_at().inject()?;
    if new_ts <= old_ts {
        return Err(SessionError::capture(
            SessionErrorKind::InvalidSnapshotTimestampOrdering {
                parent: old_ts,
                child: new_ts,
            },
        ));
    }
    let new_snapshot = Arc::new(new_snapshot);
    let new_id = new_snapshot.id();
    asset_manager.write_snapshot(Arc::clone(&new_snapshot)).await.inject()?;

    let logs: Vec<Arc<TransactionLog>> =
        sources.iter().map(|s| Arc::clone(&s.log)).collect();
    let log_id = new_id.clone();
    let merged_log = tokio::task::spawn_blocking(move || {
        TransactionLog::merge(&log_id, logs.iter().map(|l| l.as_ref()))
    })
    .await
    .capture()?
    .inject()?;
    asset_manager
        .write_transaction_log(new_id.clone(), Arc::new(merged_log))
        .await
        .inject()?;

    match do_commit_v2(
        Arc::clone(&asset_manager),
        branch,
        &plan.tip,
        new_snapshot,
        CommitMethod::NewCommit,
        false,
        config.repo_update_retries().retries(),
        config.num_updates_per_repo_info_file(),
    )
    .await
    {
        Ok(_) => {}
        Err(RepositoryError {
            kind: RepositoryErrorKind::Conflict { expected_parent, actual_parent },
            context,
        }) => {
            return Err(ICError {
                kind: SessionErrorKind::Conflict { expected_parent, actual_parent },
                context,
            });
        }
        Err(err) => return Err(err).inject(),
    }
    info!(%new_id, "Merge done");
    Ok(new_id)
}

/// Merge `snapshots` into one commit on `branch`.
pub async fn merge_snapshots(
    asset_manager: Arc<AssetManager>,
    config: &RepositoryConfig,
    branch: &str,
    snapshots: &[SnapshotId],
    message: &str,
    properties: SnapshotProperties,
) -> SessionResult<SnapshotId> {
    let (repo_info, _) = asset_manager.fetch_repo_info().await.inject()?;
    let plan = plan_merge(repo_info.as_ref(), branch, snapshots)?;
    merge_planned(asset_manager, config, branch, plan, message, properties).await
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, error::Error};

    use bytes::Bytes;
    use icechunk_macros::tokio_test;

    use super::*;
    use crate::{
        Repository,
        format::{
            ChunkIndices, Path,
            format_constants::SpecVersionBin,
            manifest::ChunkPayload,
            snapshot::{ArrayShape, NodeData},
        },
        new_in_memory_storage,
    };

    async fn create_repo() -> Repository {
        let storage =
            new_in_memory_storage().await.expect("failed to create in-memory store");
        Repository::create(None, storage, HashMap::new(), Some(SpecVersionBin::V2), true)
            .await
            .expect("failed to create repository")
    }

    /// Four chunks along one dimension.
    fn shape() -> ArrayShape {
        ArrayShape::new(vec![(4, 4)]).expect("valid shape")
    }

    /// A repo with a root group and `/array` committed on main.
    async fn repo_with_array() -> Result<(Repository, Path), Box<dyn Error>> {
        let repo = create_repo().await;
        let path: Path = "/array".try_into()?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        session.add_array(path.clone(), shape(), None, Bytes::new()).await?;
        session.commit("create array").execute().await?;
        Ok((repo, path))
    }

    /// Write one inline chunk from the tip of main and flush it as a detached snapshot.
    async fn flush_chunk(
        repo: &Repository,
        path: &Path,
        index: u32,
        value: &str,
    ) -> Result<SnapshotId, Box<dyn Error>> {
        let mut session = repo.writable_session("main").await?;
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![index]),
                Some(ChunkPayload::Inline(value.to_owned().into())),
            )
            .await?;
        Ok(session.commit(format!("chunk {index}")).anonymous().execute().await?)
    }

    /// Write one inline chunk and commit it on main.
    async fn commit_chunk(
        repo: &Repository,
        path: &Path,
        index: u32,
        value: &str,
    ) -> Result<SnapshotId, Box<dyn Error>> {
        let mut session = repo.writable_session("main").await?;
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![index]),
                Some(ChunkPayload::Inline(value.to_owned().into())),
            )
            .await?;
        Ok(session.commit(format!("chunk {index}")).execute().await?)
    }

    /// Load a flushed snapshot as a merge source.
    async fn load_source(
        repo: &Repository,
        id: &SnapshotId,
    ) -> Result<Source, Box<dyn Error>> {
        let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;
        let parent =
            repo_info.find_snapshot(id)?.parent_id.ok_or("source has no parent")?;
        Ok(Source {
            id: id.clone(),
            parent,
            snapshot: repo.asset_manager().fetch_snapshot(id).await?,
            log: repo.asset_manager().fetch_transaction_log(id).await?,
        })
    }

    #[tokio_test]
    async fn merge_nodes_applies_structure_changes() -> Result<(), Box<dyn Error>> {
        let (repo, array) = repo_with_array().await?;
        let tip_id = repo.lookup_branch("main").await?;

        // one source per change kind
        let mut session = repo.writable_session("main").await?;
        session.add_group(path("/group"), Bytes::from_static(b"g")).await?;
        let new_group = session.commit("group").anonymous().execute().await?;

        let mut session = repo.writable_session("main").await?;
        session.add_array(path("/fresh"), shape(), None, Bytes::new()).await?;
        session.set_chunk_ref(path("/fresh"), ChunkIndices(vec![0]), inline("f")).await?;
        let new_array = session.commit("fresh").anonymous().execute().await?;

        let mut session = repo.writable_session("main").await?;
        session.update_group(&Path::root(), Bytes::from_static(b"root")).await?;
        let updated = session.commit("root attrs").anonymous().execute().await?;

        let mut session = repo.writable_session("main").await?;
        session.delete_array(array.clone()).await?;
        let deleted = session.commit("delete").anonymous().execute().await?;

        let tip = repo.asset_manager().fetch_snapshot(&tip_id).await?;
        let mut sources = Vec::new();
        for id in [&new_group, &new_array, &updated, &deleted] {
            sources.push(load_source(&repo, id).await?);
        }

        let merged = merge_nodes(tip.as_ref(), &sources)?;

        let paths: Vec<&String> = merged.nodes.keys().collect();
        assert_eq!(paths, vec!["/", "/fresh", "/group"]);
        assert_eq!(merged.nodes["/"].user_data, Bytes::from_static(b"root"));
        assert_eq!(merged.nodes["/group"].user_data, Bytes::from_static(b"g"));
        let fresh_snapshot = repo.asset_manager().fetch_snapshot(&new_array).await?;
        let fresh_in_source = fresh_snapshot.get_node(&path("/fresh"))?;
        assert_eq!(merged.nodes["/fresh"].node_data, fresh_in_source.node_data);
        let NodeData::Array { manifests, .. } = &merged.nodes["/fresh"].node_data else {
            panic!("fresh is an array");
        };
        assert_eq!(manifests.len(), 1);
        assert!(merged.files.contains_key(&manifests[0].object_id));
        Ok(())
    }

    #[tokio_test]
    async fn merge_nodes_keeps_tip_manifests_for_updated_arrays()
    -> Result<(), Box<dyn Error>> {
        let (repo, array) = repo_with_array().await?;
        commit_chunk(&repo, &array, 0, "t").await?;
        let tip_id = repo.lookup_branch("main").await?;
        let tip = repo.asset_manager().fetch_snapshot(&tip_id).await?;

        let mut session = repo.writable_session("main").await?;
        session.update_array(&array, shape(), None, Bytes::from_static(b"attrs")).await?;
        let updated = session.commit("attrs").anonymous().execute().await?;
        let sources = vec![load_source(&repo, &updated).await?];

        let merged = merge_nodes(tip.as_ref(), &sources)?;

        let node = &merged.nodes["/array"];
        assert_eq!(node.user_data, Bytes::from_static(b"attrs"));
        let tip_node = tip.get_node(&array)?;
        let (
            NodeData::Array { manifests, .. },
            NodeData::Array { manifests: tip_manifests, .. },
        ) = (&node.node_data, &tip_node.node_data)
        else {
            panic!("both are arrays");
        };
        assert_eq!(manifests, tip_manifests);
        assert_eq!(manifests.len(), 1);
        Ok(())
    }

    #[tokio_test]
    async fn plan_with_sources_at_the_tip() -> Result<(), Box<dyn Error>> {
        let (repo, path) = repo_with_array().await?;
        let tip = repo.lookup_branch("main").await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        let b = flush_chunk(&repo, &path, 1, "b").await?;
        let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;

        let plan = plan_merge(&repo_info, "main", &[a.clone(), b.clone()])?;

        assert_eq!(plan.tip, tip);
        assert_eq!(plan.sources.len(), 2);
        assert_eq!(plan.sources[0].id, a);
        assert_eq!(plan.sources[0].parent, tip);
        assert!(plan.sources[0].intervening.is_empty());
        assert_eq!(plan.sources[1].id, b);
        assert!(plan.sources[1].intervening.is_empty());
        Ok(())
    }

    #[tokio_test]
    async fn plan_lists_intervening_commits_oldest_first() -> Result<(), Box<dyn Error>> {
        let (repo, path) = repo_with_array().await?;
        let parent = repo.lookup_branch("main").await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        let c1 = commit_chunk(&repo, &path, 2, "c1").await?;
        let c2 = commit_chunk(&repo, &path, 3, "c2").await?;
        let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;

        let plan = plan_merge(&repo_info, "main", std::slice::from_ref(&a))?;

        assert_eq!(plan.tip, c2);
        assert_eq!(plan.sources[0].parent, parent);
        let ids: Vec<SnapshotId> =
            plan.sources[0].intervening.iter().map(|info| info.id.clone()).collect();
        assert_eq!(ids, vec![c1, c2]);
        Ok(())
    }

    #[tokio_test]
    async fn plan_rejects_source_outside_branch_history() -> Result<(), Box<dyn Error>> {
        let (repo, path) = repo_with_array().await?;
        let tip = repo.lookup_branch("main").await?;
        repo.create_branch("other", &tip).await?;
        let mut session = repo.writable_session("other").await?;
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![0]),
                Some(ChunkPayload::Inline("o".into())),
            )
            .await?;
        session.commit("on other").execute().await?;
        let mut session = repo.writable_session("other").await?;
        session
            .set_chunk_ref(
                path.clone(),
                ChunkIndices(vec![1]),
                Some(ChunkPayload::Inline("o".into())),
            )
            .await?;
        let source = session.commit("flush on other").anonymous().execute().await?;
        let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;

        let err =
            plan_merge(&repo_info, "main", std::slice::from_ref(&source)).unwrap_err();

        assert!(matches!(
            err.kind,
            SessionErrorKind::SnapshotNotInBranchHistory { ref snapshot, ref branch }
                if snapshot == &source && branch == "main"
        ));
        Ok(())
    }

    #[tokio_test]
    async fn plan_rejects_empty_source_list() -> Result<(), Box<dyn Error>> {
        let (repo, _) = repo_with_array().await?;
        let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;

        let err = plan_merge(&repo_info, "main", &[]).unwrap_err();

        assert!(matches!(err.kind, SessionErrorKind::NoSnapshotsToMerge));
        Ok(())
    }

    use std::collections::HashSet;

    use crate::{
        change_set::{ArrayData, ChangeSet, transaction_log_from_change_set},
        format::{NodeId, transaction_log::TransactionLog},
    };

    fn path(s: &str) -> Path {
        Path::try_from(s).expect("valid path")
    }

    fn array_data() -> ArrayData {
        ArrayData { shape: shape(), dimension_names: None, user_data: Bytes::new() }
    }

    fn inline(value: &str) -> Option<ChunkPayload> {
        Some(ChunkPayload::Inline(value.to_owned().into()))
    }

    fn log(cs: &ChangeSet, id: &SnapshotId) -> TransactionLog {
        transaction_log_from_change_set(id, cs)
    }

    /// Run detection with `a` as previous and `b` as current.
    fn conflicts(
        a: &ChangeSet,
        b: &ChangeSet,
        paths: &PathIndex,
    ) -> Result<(SnapshotId, SnapshotId, Vec<MergeConflict>), Box<dyn Error>> {
        let (sa, sb) = (SnapshotId::random(), SnapshotId::random());
        let (la, lb) = (log(a, &sa), log(b, &sb));
        let found = detect_conflicts(
            &LogView { id: &sa, log: &la },
            &LogView { id: &sb, log: &lb },
            paths,
        )?;
        Ok((sa, sb, found))
    }

    #[test]
    fn disjoint_chunk_writes_do_not_conflict() -> Result<(), Box<dyn Error>> {
        let id = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.set_chunk_ref(id.clone(), ChunkIndices(vec![0]), inline("a"))?;
        let mut b = ChangeSet::for_edits();
        b.set_chunk_ref(id.clone(), ChunkIndices(vec![1]), inline("b"))?;
        let paths = PathIndex::from_pairs([(id, path("/array"))]);

        let (_, _, found) = conflicts(&a, &b, &paths)?;

        assert!(found.is_empty());
        Ok(())
    }

    #[test]
    fn detects_chunk_double_update() -> Result<(), Box<dyn Error>> {
        let id = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.set_chunk_ref(id.clone(), ChunkIndices(vec![0]), inline("a"))?;
        a.set_chunk_ref(id.clone(), ChunkIndices(vec![1]), inline("a"))?;
        let mut b = ChangeSet::for_edits();
        b.set_chunk_ref(id.clone(), ChunkIndices(vec![1]), inline("b"))?;
        b.set_chunk_ref(id.clone(), ChunkIndices(vec![2]), inline("b"))?;
        let paths = PathIndex::from_pairs([(id.clone(), path("/array"))]);

        let (sa, sb, found) = conflicts(&a, &b, &paths)?;

        assert_eq!(
            found,
            vec![MergeConflict {
                first_snapshot: sa,
                second_snapshot: sb,
                conflict: Conflict::ChunkDoubleUpdate {
                    path: path("/array"),
                    node_id: id,
                    chunk_coordinates: HashSet::from([ChunkIndices(vec![1])]),
                },
            }]
        );
        Ok(())
    }

    #[test]
    fn detects_metadata_double_update() -> Result<(), Box<dyn Error>> {
        let array = NodeId::random();
        let group = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.update_array(&array, &path("/array"), array_data())?;
        a.update_group(&group, &path("/group"), Bytes::from_static(b"a"))?;
        let mut b = ChangeSet::for_edits();
        b.update_array(&array, &path("/array"), array_data())?;
        b.update_group(&group, &path("/group"), Bytes::from_static(b"b"))?;
        let paths =
            PathIndex::from_pairs([(array, path("/array")), (group, path("/group"))]);

        let (_, _, found) = conflicts(&a, &b, &paths)?;

        let kinds: Vec<&Conflict> = found.iter().map(|c| &c.conflict).collect();
        assert_eq!(
            kinds,
            vec![
                &Conflict::ZarrMetadataDoubleUpdate(path("/array")),
                &Conflict::ZarrMetadataDoubleUpdate(path("/group")),
            ]
        );
        Ok(())
    }

    #[test]
    fn detects_update_of_deleted_nodes() -> Result<(), Box<dyn Error>> {
        let array = NodeId::random();
        let group = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.delete_array(path("/array"), &array)?;
        a.delete_group(path("/group"), &group)?;
        let mut b = ChangeSet::for_edits();
        b.update_array(&array, &path("/array"), array_data())?;
        b.update_group(&group, &path("/group"), Bytes::from_static(b"b"))?;
        let paths =
            PathIndex::from_pairs([(array, path("/array")), (group, path("/group"))]);

        let (_, _, found) = conflicts(&a, &b, &paths)?;

        let kinds: Vec<&Conflict> = found.iter().map(|c| &c.conflict).collect();
        assert_eq!(
            kinds,
            vec![
                &Conflict::ZarrMetadataUpdateOfDeletedArray(path("/array")),
                &Conflict::ZarrMetadataUpdateOfDeletedGroup(path("/group")),
            ]
        );
        Ok(())
    }

    #[test]
    fn detects_chunks_in_deleted_and_updated_arrays() -> Result<(), Box<dyn Error>> {
        let deleted = NodeId::random();
        let updated = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.delete_array(path("/deleted"), &deleted)?;
        a.update_array(&updated, &path("/updated"), array_data())?;
        let mut b = ChangeSet::for_edits();
        b.set_chunk_ref(deleted.clone(), ChunkIndices(vec![0]), inline("b"))?;
        b.set_chunk_ref(updated.clone(), ChunkIndices(vec![0]), inline("b"))?;
        let paths = PathIndex::from_pairs([
            (deleted.clone(), path("/deleted")),
            (updated.clone(), path("/updated")),
        ]);

        let (_, _, mut found) = conflicts(&a, &b, &paths)?;

        found.sort_by_key(|c| format!("{:?}", c.conflict));
        let kinds: Vec<&Conflict> = found.iter().map(|c| &c.conflict).collect();
        assert_eq!(
            kinds,
            vec![
                &Conflict::ChunksUpdatedInDeletedArray {
                    path: path("/deleted"),
                    node_id: deleted,
                },
                &Conflict::ChunksUpdatedInUpdatedArray {
                    path: path("/updated"),
                    node_id: updated,
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn detects_delete_of_updated_nodes() -> Result<(), Box<dyn Error>> {
        let array = NodeId::random();
        let written = NodeId::random();
        let group = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.update_array(&array, &path("/array"), array_data())?;
        a.set_chunk_ref(written.clone(), ChunkIndices(vec![0]), inline("a"))?;
        a.update_group(&group, &path("/group"), Bytes::from_static(b"a"))?;
        let mut b = ChangeSet::for_edits();
        b.delete_array(path("/array"), &array)?;
        b.delete_array(path("/written"), &written)?;
        b.delete_group(path("/group"), &group)?;
        let paths = PathIndex::from_pairs([
            (array.clone(), path("/array")),
            (written.clone(), path("/written")),
            (group.clone(), path("/group")),
        ]);

        let (_, _, mut found) = conflicts(&a, &b, &paths)?;

        found.sort_by_key(|c| format!("{:?}", c.conflict));
        let kinds: Vec<&Conflict> = found.iter().map(|c| &c.conflict).collect();
        assert_eq!(
            kinds,
            vec![
                &Conflict::DeleteOfUpdatedArray { path: path("/array"), node_id: array },
                &Conflict::DeleteOfUpdatedArray {
                    path: path("/written"),
                    node_id: written,
                },
                &Conflict::DeleteOfUpdatedGroup { path: path("/group"), node_id: group },
            ]
        );
        Ok(())
    }

    #[test]
    fn detects_new_node_conflicts() -> Result<(), Box<dyn Error>> {
        let (a_same, b_same) = (NodeId::random(), NodeId::random());
        let (a_parent, b_child) = (NodeId::random(), NodeId::random());
        let mut a = ChangeSet::for_edits();
        a.add_group(path("/same"), a_same.clone(), Bytes::new())?;
        a.add_array(path("/parent"), a_parent.clone(), array_data())?;
        let mut b = ChangeSet::for_edits();
        b.add_array(path("/same"), b_same.clone(), array_data())?;
        b.add_group(path("/parent/child"), b_child.clone(), Bytes::new())?;
        let paths = PathIndex::from_pairs([
            (a_same, path("/same")),
            (b_same, path("/same")),
            (a_parent, path("/parent")),
            (b_child, path("/parent/child")),
        ]);

        let (_, _, mut found) = conflicts(&a, &b, &paths)?;

        found.sort_by_key(|c| format!("{:?}", c.conflict));
        let kinds: Vec<&Conflict> = found.iter().map(|c| &c.conflict).collect();
        assert_eq!(
            kinds,
            vec![
                &Conflict::NewNodeConflictsWithExistingNode(path("/same")),
                &Conflict::NewNodeInInvalidGroup(path("/parent")),
            ]
        );
        Ok(())
    }

    #[test]
    fn unresolvable_node_id_is_an_error() -> Result<(), Box<dyn Error>> {
        let id = NodeId::random();
        let mut a = ChangeSet::for_edits();
        a.update_array(&id, &path("/array"), array_data())?;
        let mut b = ChangeSet::for_edits();
        b.update_array(&id, &path("/array"), array_data())?;
        let paths = PathIndex::from_pairs([]);
        let (sa, sb) = (SnapshotId::random(), SnapshotId::random());
        let (la, lb) = (log(&a, &sa), log(&b, &sb));

        let err = detect_conflicts(
            &LogView { id: &sa, log: &la },
            &LogView { id: &sb, log: &lb },
            &paths,
        )
        .unwrap_err();

        assert!(
            matches!(err.kind, SessionErrorKind::ConflictingPathNotFound(ref n) if n == &id)
        );
        Ok(())
    }

    use crate::{
        RepositoryConfig,
        config::{ManifestConfig, ManifestSplittingConfig},
        format::manifest::ManifestRef,
    };

    /// Repo whose manifests hold two chunks each, with `/array` of four chunks.
    async fn split_repo_with_array() -> Result<(Repository, Path), Box<dyn Error>> {
        let storage = new_in_memory_storage().await?;
        let config = RepositoryConfig {
            manifest: Some(ManifestConfig {
                splitting: Some(ManifestSplittingConfig::with_size(2)),
                ..ManifestConfig::default()
            }),
            ..RepositoryConfig::default()
        };
        let repo = Repository::create(
            Some(config),
            storage,
            HashMap::new(),
            Some(SpecVersionBin::V2),
            true,
        )
        .await?;
        let path: Path = "/array".try_into()?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        session.add_array(path.clone(), shape(), None, Bytes::new()).await?;
        session.commit("create array").execute().await?;
        Ok((repo, path))
    }

    async fn manifest_count(repo: &Repository) -> Result<usize, Box<dyn Error>> {
        use futures::StreamExt as _;
        Ok(repo.asset_manager().list_manifests().await?.count().await)
    }

    fn array_refs(snapshot: &Snapshot, path: &Path) -> Vec<ManifestRef> {
        match &snapshot.get_node(path).expect("node exists").node_data {
            NodeData::Array { manifests, .. } => manifests.clone(),
            NodeData::Group => panic!("not an array"),
        }
    }

    #[tokio_test]
    async fn disjoint_splits_reuse_source_manifests() -> Result<(), Box<dyn Error>> {
        let (repo, path) = split_repo_with_array().await?;
        let tip_id = repo.lookup_branch("main").await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        let b = flush_chunk(&repo, &path, 3, "b").await?;
        let tip = repo.asset_manager().fetch_snapshot(&tip_id).await?;
        let sources = [load_source(&repo, &a).await?, load_source(&repo, &b).await?];
        let node = tip.get_node(&path)?;
        let before = manifest_count(&repo).await?;

        let merged = merge_array_manifests(
            repo.asset_manager().as_ref(),
            repo.config(),
            tip.as_ref(),
            node.as_ref(),
            &sources.iter().collect::<Vec<_>>(),
        )
        .await?;

        assert_eq!(manifest_count(&repo).await?, before, "no manifest written");
        let mut expected: Vec<ManifestRef> = array_refs(&sources[0].snapshot, &path);
        expected.extend(array_refs(&sources[1].snapshot, &path));
        let mut got = merged.refs.clone();
        got.sort_by(|x, y| x.object_id.cmp(&y.object_id));
        expected.sort_by(|x, y| x.object_id.cmp(&y.object_id));
        assert_eq!(got, expected);
        assert_eq!(merged.files.len(), 2);
        Ok(())
    }

    #[tokio_test]
    async fn shared_split_writes_one_merged_manifest() -> Result<(), Box<dyn Error>> {
        let (repo, path) = split_repo_with_array().await?;
        commit_chunk(&repo, &path, 1, "tip").await?;
        let tip_id = repo.lookup_branch("main").await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        let b = flush_chunk(&repo, &path, 1, "b").await?; // overwrites the tip's chunk 1
        let tip = repo.asset_manager().fetch_snapshot(&tip_id).await?;
        let sources = [load_source(&repo, &a).await?, load_source(&repo, &b).await?];
        let node = tip.get_node(&path)?;

        let merged = merge_array_manifests(
            repo.asset_manager().as_ref(),
            repo.config(),
            tip.as_ref(),
            node.as_ref(),
            &sources.iter().collect::<Vec<_>>(),
        )
        .await?;

        assert_eq!(merged.refs.len(), 1);
        let info = &merged.files[0];
        let manifest = repo
            .asset_manager()
            .fetch_manifest(&merged.refs[0].object_id, info.size_bytes)
            .await?;
        assert_eq!(
            manifest.get_chunk_payload(&node.id, &ChunkIndices(vec![0]))?,
            ChunkPayload::Inline("a".into())
        );
        assert_eq!(
            manifest.get_chunk_payload(&node.id, &ChunkIndices(vec![1]))?,
            ChunkPayload::Inline("b".into())
        );
        Ok(())
    }

    #[tokio_test]
    async fn deletion_removes_chunk_from_merged_manifest() -> Result<(), Box<dyn Error>> {
        let (repo, path) = split_repo_with_array().await?;
        commit_chunk(&repo, &path, 0, "keep").await?;
        commit_chunk(&repo, &path, 1, "drop").await?;
        let tip_id = repo.lookup_branch("main").await?;
        let mut session = repo.writable_session("main").await?;
        session.set_chunk_ref(path.clone(), ChunkIndices(vec![1]), None).await?;
        let deleter = session.commit("delete 1").anonymous().execute().await?;
        // a second writer on the same split forces the rewrite path
        let other = flush_chunk(&repo, &path, 0, "keep2").await?;
        let tip = repo.asset_manager().fetch_snapshot(&tip_id).await?;
        let sources =
            [load_source(&repo, &deleter).await?, load_source(&repo, &other).await?];
        let node = tip.get_node(&path)?;

        let merged = merge_array_manifests(
            repo.asset_manager().as_ref(),
            repo.config(),
            tip.as_ref(),
            node.as_ref(),
            &sources.iter().collect::<Vec<_>>(),
        )
        .await?;

        assert_eq!(merged.refs.len(), 1);
        let manifest = repo
            .asset_manager()
            .fetch_manifest(&merged.refs[0].object_id, merged.files[0].size_bytes)
            .await?;
        assert_eq!(
            manifest.get_chunk_payload(&node.id, &ChunkIndices(vec![0]))?,
            ChunkPayload::Inline("keep2".into())
        );
        assert!(manifest.get_chunk_payload(&node.id, &ChunkIndices(vec![1])).is_err());
        Ok(())
    }

    #[tokio_test]
    async fn moved_tip_rewrites_instead_of_reusing() -> Result<(), Box<dyn Error>> {
        let (repo, path) = split_repo_with_array().await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        commit_chunk(&repo, &path, 1, "tip").await?;
        let tip_id = repo.lookup_branch("main").await?;
        let tip = repo.asset_manager().fetch_snapshot(&tip_id).await?;
        let sources = [load_source(&repo, &a).await?];
        let node = tip.get_node(&path)?;

        let merged = merge_array_manifests(
            repo.asset_manager().as_ref(),
            repo.config(),
            tip.as_ref(),
            node.as_ref(),
            &sources.iter().collect::<Vec<_>>(),
        )
        .await?;

        assert_eq!(merged.refs.len(), 1);
        assert_ne!(
            merged.refs[0].object_id,
            array_refs(&sources[0].snapshot, &path)[0].object_id
        );
        let manifest = repo
            .asset_manager()
            .fetch_manifest(&merged.refs[0].object_id, merged.files[0].size_bytes)
            .await?;
        assert_eq!(
            manifest.get_chunk_payload(&node.id, &ChunkIndices(vec![0]))?,
            ChunkPayload::Inline("a".into())
        );
        assert_eq!(
            manifest.get_chunk_payload(&node.id, &ChunkIndices(vec![1]))?,
            ChunkPayload::Inline("tip".into())
        );
        Ok(())
    }

    /// A worker splitting more finely than the coordinator can leave more than
    /// one of its manifests inside a single coordinator split; the fast path
    /// must not reuse either of them, or the untouched chunk they don't cover
    /// individually would be dropped.
    #[tokio_test]
    async fn finer_source_splits_keep_untouched_chunk() -> Result<(), Box<dyn Error>> {
        let storage = new_in_memory_storage().await?;
        let coordinator_config = RepositoryConfig {
            manifest: Some(ManifestConfig {
                splitting: Some(ManifestSplittingConfig::with_size(2)),
                ..ManifestConfig::default()
            }),
            ..RepositoryConfig::default()
        };
        let repo = Repository::create(
            Some(coordinator_config),
            Arc::clone(&storage),
            HashMap::new(),
            Some(SpecVersionBin::V2),
            true,
        )
        .await?;
        let path: Path = "/array".try_into()?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        session.add_array(path.clone(), shape(), None, Bytes::new()).await?;
        session.commit("create array").execute().await?;
        commit_chunk(&repo, &path, 0, "orig0").await?;
        commit_chunk(&repo, &path, 1, "orig1").await?;
        let tip_id = repo.lookup_branch("main").await?;
        let tip = repo.asset_manager().fetch_snapshot(&tip_id).await?;

        // Same storage and branch, but splitting one chunk per manifest.
        let worker_config = RepositoryConfig {
            manifest: Some(ManifestConfig {
                splitting: Some(ManifestSplittingConfig::with_size(1)),
                ..ManifestConfig::default()
            }),
            ..RepositoryConfig::default()
        };
        let worker =
            Repository::open(Some(worker_config), Arc::clone(&storage), HashMap::new())
                .await?;
        let a = flush_chunk(&worker, &path, 0, "new0").await?;
        let source = load_source(&repo, &a).await?;
        let node = tip.get_node(&path)?;

        let merged = merge_array_manifests(
            repo.asset_manager().as_ref(),
            repo.config(),
            tip.as_ref(),
            node.as_ref(),
            &[&source],
        )
        .await?;

        assert_eq!(merged.refs.len(), 1);
        let source_manifest_ids: HashSet<ManifestId> =
            array_refs(&source.snapshot, &path)
                .into_iter()
                .map(|r| r.object_id)
                .collect();
        assert!(!source_manifest_ids.contains(&merged.refs[0].object_id));
        let manifest = repo
            .asset_manager()
            .fetch_manifest(&merged.refs[0].object_id, merged.files[0].size_bytes)
            .await?;
        assert_eq!(
            manifest.get_chunk_payload(&node.id, &ChunkIndices(vec![0]))?,
            ChunkPayload::Inline("new0".into())
        );
        assert_eq!(
            manifest.get_chunk_payload(&node.id, &ChunkIndices(vec![1]))?,
            ChunkPayload::Inline("orig1".into())
        );
        Ok(())
    }

    use std::num::{NonZeroU16, NonZeroUsize};

    use chrono::Utc;

    use crate::{
        format::ByteRange,
        ops::gc::{GCConfig, garbage_collect},
        repository::VersionInfo,
        session::get_chunk,
    };

    async fn read_chunk(
        repo: &Repository,
        path: &Path,
        index: u32,
    ) -> Result<Option<Bytes>, Box<dyn Error>> {
        let session =
            repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;
        let reader = session
            .get_chunk_reader(path, &ChunkIndices(vec![index]), &ByteRange::ALL)
            .await?;
        Ok(get_chunk(reader).await?)
    }

    #[tokio_test]
    async fn merge_disjoint_writers() -> Result<(), Box<dyn Error>> {
        let (repo, path) = repo_with_array().await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        let b = flush_chunk(&repo, &path, 1, "b").await?;
        let meta: SnapshotProperties = [("job".to_string(), 7.into())].into();

        let merged =
            repo.merge_snapshots("main", &[a, b], "merge", Some(meta.clone())).await?;

        assert_eq!(read_chunk(&repo, &path, 0).await?, Some(Bytes::from_static(b"a")));
        assert_eq!(read_chunk(&repo, &path, 1).await?, Some(Bytes::from_static(b"b")));
        assert_eq!(read_chunk(&repo, &path, 2).await?, None);
        let history: Vec<_> = repo
            .ancestry(&VersionInfo::BranchTipRef("main".to_string()))
            .await?
            .try_collect()
            .await?;
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].id, merged);
        assert_eq!(history[0].message, "merge");
        assert_eq!(history[0].metadata, meta);
        Ok(())
    }

    #[tokio_test]
    async fn merge_reuses_manifests_and_gc_removes_sources() -> Result<(), Box<dyn Error>>
    {
        let (repo, path) = split_repo_with_array().await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        let b = flush_chunk(&repo, &path, 3, "b").await?;
        let source_a = repo.asset_manager().fetch_snapshot(&a).await?;
        let reused = array_refs(&source_a, &path)[0].object_id.clone();
        let before = manifest_count(&repo).await?;

        let merged =
            repo.merge_snapshots("main", &[a.clone(), b.clone()], "merge", None).await?;

        assert_eq!(manifest_count(&repo).await?, before);
        let merged_snapshot = repo.asset_manager().fetch_snapshot(&merged).await?;
        assert!(
            array_refs(&merged_snapshot, &path).iter().any(|m| m.object_id == reused)
        );

        let later = Utc::now() + chrono::Duration::hours(1);
        let gc_config = GCConfig::clean_all(
            later,
            later,
            None,
            NonZeroU16::new(50).ok_or("nonzero")?,
            NonZeroUsize::new(512 * 1024 * 1024).ok_or("nonzero")?,
            NonZeroU16::new(500).ok_or("nonzero")?,
            false,
        );
        let summary =
            garbage_collect(Arc::clone(repo.asset_manager()), &gc_config, None, 100)
                .await?;
        assert_eq!(summary.snapshots_deleted, 2);
        assert_eq!(summary.manifests_deleted, 0);
        let remaining: Vec<SnapshotId> = repo
            .asset_manager()
            .list_snapshots()
            .await?
            .map_ok(|info| info.id)
            .try_collect()
            .await?;
        assert!(!remaining.contains(&a));
        assert!(!remaining.contains(&b));
        assert!(remaining.contains(&merged));
        assert_eq!(read_chunk(&repo, &path, 0).await?, Some(Bytes::from_static(b"a")));
        Ok(())
    }

    #[tokio_test]
    async fn merge_onto_moved_tip_without_conflict() -> Result<(), Box<dyn Error>> {
        let (repo, path) = repo_with_array().await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        commit_chunk(&repo, &path, 2, "c").await?;

        repo.merge_snapshots("main", &[a], "merge", None).await?;

        assert_eq!(read_chunk(&repo, &path, 0).await?, Some(Bytes::from_static(b"a")));
        assert_eq!(read_chunk(&repo, &path, 2).await?, Some(Bytes::from_static(b"c")));
        Ok(())
    }

    #[tokio_test]
    async fn merge_onto_moved_tip_with_conflict() -> Result<(), Box<dyn Error>> {
        let (repo, path) = repo_with_array().await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        let c = commit_chunk(&repo, &path, 0, "c").await?;

        let err = repo
            .merge_snapshots("main", std::slice::from_ref(&a), "merge", None)
            .await
            .unwrap_err();

        let SessionErrorKind::MergeConflict { conflicts } = err.kind else {
            panic!("expected MergeConflict, got {err:?}");
        };
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].first_snapshot, c);
        assert_eq!(conflicts[0].second_snapshot, a);
        assert!(matches!(conflicts[0].conflict, Conflict::ChunkDoubleUpdate { .. }));
        Ok(())
    }

    #[tokio_test]
    async fn merge_reports_every_conflict() -> Result<(), Box<dyn Error>> {
        let (repo, path) = repo_with_array().await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        let b = flush_chunk(&repo, &path, 0, "b").await?;
        let c = flush_chunk(&repo, &path, 0, "c").await?;

        let err = repo
            .merge_snapshots("main", &[a.clone(), b.clone(), c.clone()], "merge", None)
            .await
            .unwrap_err();

        let SessionErrorKind::MergeConflict { conflicts } = err.kind else {
            panic!("expected MergeConflict, got {err:?}");
        };
        let pairs: Vec<(SnapshotId, SnapshotId)> = conflicts
            .iter()
            .map(|c| (c.first_snapshot.clone(), c.second_snapshot.clone()))
            .collect();
        assert_eq!(pairs, vec![(a.clone(), b.clone()), (a, c.clone()), (b, c)]);
        Ok(())
    }

    #[tokio_test]
    async fn merge_fails_on_pruned_ancestor_conflict() -> Result<(), Box<dyn Error>> {
        use std::time::Duration;

        use crate::ops::gc::{ExpiredRefAction, expire};

        // The source and the expired commit both create `/array`, which is the
        // conflict. Expiration releases every snapshot older than the cutoff
        // except roots, so the source branches off the initial snapshot and is
        // written after the cutoff.
        let repo = create_repo().await;
        let array = path("/array");
        let initial = repo.lookup_branch("main").await?;

        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        session.add_array(array.clone(), shape(), None, Bytes::new()).await?;
        let c1 = session.commit("c1").execute().await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
        let expire_older_than = Utc::now();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let c2 = commit_chunk(&repo, &array, 3, "c2").await?;

        repo.create_branch("worker", &initial).await?;
        let mut session = repo.writable_session("worker").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        session.add_array(array.clone(), shape(), None, Bytes::new()).await?;
        session.set_chunk_ref(array.clone(), ChunkIndices(vec![0]), inline("a")).await?;
        let a = session.commit("source").anonymous().execute().await?;

        let result = expire(
            Arc::clone(repo.asset_manager()),
            expire_older_than,
            ExpiredRefAction::Ignore,
            ExpiredRefAction::Ignore,
            None,
            100,
        )
        .await?;
        assert!(result.released_snapshots.contains(&c1));
        let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;
        assert_eq!(
            repo_info.find_snapshot(&c2)?.pruned_ancestor_tx_logs,
            vec![c1.clone()]
        );

        let err = repo
            .merge_snapshots("main", std::slice::from_ref(&a), "merge", None)
            .await
            .unwrap_err();

        let SessionErrorKind::MergeConflict { conflicts } = err.kind else {
            panic!("expected MergeConflict, got {err:?}");
        };
        assert!(conflicts.iter().any(|c| {
            c.first_snapshot == c1
                && c.second_snapshot == a
                && c.conflict == Conflict::NewNodeConflictsWithExistingNode(array.clone())
        }));
        Ok(())
    }

    #[tokio_test]
    async fn merge_fails_when_tip_moves_after_planning() -> Result<(), Box<dyn Error>> {
        let (repo, path) = repo_with_array().await?;
        let old_tip = repo.lookup_branch("main").await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;
        let (repo_info, _) = repo.asset_manager().fetch_repo_info().await?;
        let plan = plan_merge(&repo_info, "main", &[a])?;
        let new_tip = commit_chunk(&repo, &path, 2, "c").await?;

        let err = merge_planned(
            Arc::clone(repo.asset_manager()),
            repo.config(),
            "main",
            plan,
            "merge",
            SnapshotProperties::default(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err.kind,
            SessionErrorKind::Conflict { ref expected_parent, ref actual_parent }
                if expected_parent == &Some(old_tip.clone()) && actual_parent == &Some(new_tip.clone())
        ));
        Ok(())
    }

    #[tokio_test]
    async fn merge_rejects_v1_repository() -> Result<(), Box<dyn Error>> {
        let storage = new_in_memory_storage().await?;
        let repo = Repository::create(
            None,
            storage,
            HashMap::new(),
            Some(SpecVersionBin::V1),
            true,
        )
        .await?;
        let path: Path = "/array".try_into()?;
        let mut session = repo.writable_session("main").await?;
        session.add_group(Path::root(), Bytes::new()).await?;
        session.add_array(path.clone(), shape(), None, Bytes::new()).await?;
        session.commit("create array").execute().await?;
        let a = flush_chunk(&repo, &path, 0, "a").await?;

        let err = repo.merge_snapshots("main", &[a], "merge", None).await.unwrap_err();

        assert!(matches!(
            err.kind,
            SessionErrorKind::RepositoryError(RepositoryErrorKind::BadRepoVersion { .. })
        ));
        Ok(())
    }

    #[tokio_test]
    async fn merge_rejects_empty_list() -> Result<(), Box<dyn Error>> {
        let (repo, _) = repo_with_array().await?;

        let err = repo.merge_snapshots("main", &[], "merge", None).await.unwrap_err();

        assert!(matches!(err.kind, SessionErrorKind::NoSnapshotsToMerge));
        Ok(())
    }

    #[tokio_test]
    async fn merge_applies_structure_and_deletions_end_to_end()
    -> Result<(), Box<dyn Error>> {
        let (repo, array) = repo_with_array().await?;
        commit_chunk(&repo, &array, 1, "drop").await?;

        let mut session = repo.writable_session("main").await?;
        session.add_group(path("/group"), Bytes::from_static(b"g")).await?;
        let new_group = session.commit("group").anonymous().execute().await?;
        let mut session = repo.writable_session("main").await?;
        session.set_chunk_ref(array.clone(), ChunkIndices(vec![1]), None).await?;
        let deleter = session.commit("delete chunk").anonymous().execute().await?;
        let mut session = repo.writable_session("main").await?;
        session.add_array(path("/fresh"), shape(), None, Bytes::new()).await?;
        session.set_chunk_ref(path("/fresh"), ChunkIndices(vec![0]), inline("f")).await?;
        let fresh = session.commit("fresh").anonymous().execute().await?;

        repo.merge_snapshots("main", &[new_group, deleter, fresh], "merge", None).await?;

        assert_eq!(read_chunk(&repo, &array, 1).await?, None);
        assert_eq!(
            read_chunk(&repo, &path("/fresh"), 0).await?,
            Some(Bytes::from_static(b"f"))
        );
        let session =
            repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())).await?;
        assert_eq!(
            session.get_group(&path("/group")).await?.user_data,
            Bytes::from_static(b"g")
        );
        Ok(())
    }
}
