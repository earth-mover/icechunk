//! Tracks uncommitted modifications during a session.
//!
//! Key types:
//! - [`ChangeSet`] - Enum with variants for the two writable session modes
//! - [`ArrayData`] - Array metadata (shape, dimension names, user attributes)

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    iter,
    sync::LazyLock,
};

use bytes::Bytes;
use itertools::{Either, Itertools as _};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    format::{
        ChunkIndices, NodeId, Path,
        manifest::{ChunkInfo, ChunkPayload, ManifestExtents, ManifestSplits, Overlap},
        snapshot::{ArrayShape, DimensionName, NodeData, NodeSnapshot},
    },
    session::{SessionErrorKind, SessionResult, find_coord},
};

// We have limitations on how many chunks we can save on a single commit.
// Mostly due to flatbuffers not supporting 64-bit offsets,
// but also because we can't do transaction log splitting (like we can with manifests).
// For now we suggest smaller commits as a solution.
// See discussion in https://github.com/earth-mover/icechunk/issues/1558 for more details
const NUM_CHUNKS_LIMIT: u64 = 50_000_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrayData {
    pub shape: ArrayShape,
    pub dimension_names: Option<Vec<DimensionName>>,
    pub user_data: Bytes,
}

type SplitManifest = BTreeMap<ChunkIndices, Option<ChunkPayload>>;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EditChanges {
    new_groups: HashMap<Path, (NodeId, Bytes)>,
    new_arrays: HashMap<Path, (NodeId, ArrayData)>,
    updated_arrays: HashMap<NodeId, ArrayData>,
    updated_groups: HashMap<NodeId, Bytes>,
    set_chunks: BTreeMap<NodeId, HashMap<ManifestExtents, SplitManifest>>,

    // Number of chunks added to this change set
    num_chunks: u64,

    // Did we already print a warning about too many chunks in this change set?
    excessive_num_chunks_warned: bool,

    // This map keeps track of any chunk deletes that are
    // outside the domain of the current array shape. This is needed to handle
    // the very unlikely case of multiple resizes in the same session.
    deleted_chunks_outside_bounds: BTreeMap<NodeId, HashSet<ChunkIndices>>,
    deleted_groups: HashSet<(Path, NodeId)>,
    deleted_arrays: HashSet<(Path, NodeId)>,
}

impl EditChanges {
    fn is_empty(&self) -> bool {
        self == &Default::default()
    }

    fn merge(&mut self, other: EditChanges) {
        // FIXME: this should detect conflict, for example, if different writers added on the same
        // path, different objects, or if the same path is added and deleted, etc.
        // TODO: optimize
        self.new_groups.extend(other.new_groups);
        self.new_arrays.extend(other.new_arrays);
        self.updated_groups.extend(other.updated_groups);
        self.updated_arrays.extend(other.updated_arrays);
        self.deleted_groups.extend(other.deleted_groups);
        self.deleted_arrays.extend(other.deleted_arrays);
        // FIXME: do we even test this?
        self.deleted_chunks_outside_bounds.extend(other.deleted_chunks_outside_bounds);

        other.set_chunks.into_iter().for_each(|(node, other_splits)| {
            let manifests = self.set_chunks.entry(node).or_insert_with(|| {
                HashMap::<ManifestExtents, SplitManifest>::with_capacity(
                    other_splits.len(),
                )
            });
            other_splits.into_iter().for_each(|(extent, their_manifest)| {
                manifests.entry(extent).or_default().extend(their_manifest)
            })
        });
    }
}

pub static EMPTY_EDITS: LazyLock<EditChanges> = LazyLock::new(Default::default);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Move {
    pub from: Path,
    pub to: Path,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MoveTracker(Vec<Move>);

pub static EMPTY_MOVE_TRACKER: LazyLock<MoveTracker> = LazyLock::new(Default::default);

impl MoveTracker {
    pub fn record(&mut self, from: Path, to: Path) {
        self.0.push(Move { from, to })
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn all_moves(&self) -> impl Iterator<Item = &Move> {
        self.0.iter()
    }

    pub fn moved_to<'a>(&self, path: &'a Path) -> Option<Cow<'a, Path>> {
        let mut res = Cow::Borrowed(path);
        for Move { from, to } in self.0.iter() {
            if let Ok(rest) = res.as_ref().buf().strip_prefix(from.buf()) {
                // it's safe to join segments that already belonged to a Path
                #[allow(clippy::expect_used)]
                let new_path = Path::new(to.buf().join(rest).to_string().as_str())
                    .expect("Bug in moved_to, cannot create path");
                res = Cow::Owned(new_path);
            } else if res.buf().starts_with(to.buf()) {
                // the path has been overwritten by the moves
                // calling code should check for overwrites before moving
                return None;
            }
        }
        Some(res)
    }

    pub fn moved_from<'a>(&self, path: &'a Path) -> Option<Cow<'a, Path>> {
        let mut res = Cow::Borrowed(path);
        for Move { from, to } in self.0.iter().rev() {
            if let Ok(rest) = res.as_ref().buf().strip_prefix(to.buf()) {
                // it's safe to join segments that already belonged to a Path
                #[allow(clippy::expect_used)]
                let old_path = Path::new(from.buf().join(rest).to_string().as_str())
                    .expect("Bug in moved_from, cannot create path");
                res = Cow::Owned(old_path);
            } else if res.buf().starts_with(from.buf()) {
                // the moves have deleted this path
                // calling code should check for overwrites before moving
                return None;
            }
        }
        Some(res)
    }
}

/// Uncommitted modifications accumulated during a session.
///
/// Two variants for the two writable session modes:
/// - [`Edit`](ChangeSet::Edit) - New/updated/deleted arrays, groups, and chunks
/// - [`Rearrange`](ChangeSet::Rearrange) - Move/rename operations
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
// we only keep one of this, their size difference doesn't affect us
#[allow(clippy::large_enum_variant)]
pub enum ChangeSet {
    Edit(EditChanges),
    Rearrange(MoveTracker),
}

impl ChangeSet {
    pub fn for_edits() -> Self {
        ChangeSet::Edit(Default::default())
    }

    pub fn for_rearranging() -> Self {
        ChangeSet::Rearrange(Default::default())
    }

    fn edits(&self) -> &EditChanges {
        match self {
            ChangeSet::Edit(edit_changes) => edit_changes,
            ChangeSet::Rearrange(_) => &EMPTY_EDITS,
        }
    }

    fn edits_mut(&mut self) -> SessionResult<&mut EditChanges> {
        match self {
            ChangeSet::Edit(edit_changes) => Ok(edit_changes),
            ChangeSet::Rearrange(_) => Err(SessionErrorKind::RearrangeSessionOnly.into()),
        }
    }

    fn move_tracker(&self) -> &MoveTracker {
        match self {
            ChangeSet::Edit(_) => &EMPTY_MOVE_TRACKER,
            ChangeSet::Rearrange(move_tracker) => move_tracker,
        }
    }

    fn move_tracker_mut(&mut self) -> SessionResult<&mut MoveTracker> {
        match self {
            ChangeSet::Edit(_) => Err(SessionErrorKind::NonRearrangeSession.into()),
            ChangeSet::Rearrange(move_tracker) => Ok(move_tracker),
        }
    }

    pub fn discard_changes(&mut self) {
        match self {
            ChangeSet::Edit(_) => *self = Self::for_edits(),
            ChangeSet::Rearrange(_) => *self = Self::for_rearranging(),
        }
    }

    pub fn fresh(&self) -> Self {
        match self {
            ChangeSet::Edit(_) => Self::for_edits(),
            ChangeSet::Rearrange(_) => Self::for_rearranging(),
        }
    }

    pub fn moves(&self) -> impl Iterator<Item = &Move> {
        match self {
            ChangeSet::Edit(_) => Either::Left(iter::empty()),
            ChangeSet::Rearrange(move_tracker) => Either::Right(move_tracker.all_moves()),
        }
    }

    pub fn deleted_arrays(&self) -> impl Iterator<Item = &(Path, NodeId)> {
        self.edits().deleted_arrays.iter()
    }

    pub fn deleted_groups(&self) -> impl Iterator<Item = &(Path, NodeId)> {
        self.edits().deleted_groups.iter()
    }

    pub fn updated_arrays(&self) -> impl Iterator<Item = &NodeId> {
        self.edits().updated_arrays.keys()
    }

    pub fn updated_groups(&self) -> impl Iterator<Item = &NodeId> {
        self.edits().updated_groups.keys()
    }

    pub fn array_is_deleted(&self, path_and_id: &(Path, NodeId)) -> bool {
        self.edits().deleted_arrays.contains(path_and_id)
    }

    pub fn changed_chunks(
        &self,
    ) -> impl Iterator<Item = (&NodeId, impl Iterator<Item = &ChunkIndices>)> {
        self.edits().set_chunks.iter().map(|(node_id, split_map)| {
            (node_id, split_map.values().flat_map(|x| x.keys()))
        })
    }

    pub fn is_updated_array(&self, node: &NodeId) -> bool {
        self.edits().updated_arrays.contains_key(node)
    }

    pub fn has_chunk_changes(&self, node: &NodeId) -> bool {
        self.edits().set_chunks.get(node).map(|m| !m.is_empty()).unwrap_or(false)
    }

    pub fn arrays_with_chunk_changes(&self) -> impl Iterator<Item = &NodeId> {
        self.edits().set_chunks.keys()
    }

    pub fn is_empty(&self) -> bool {
        match self {
            ChangeSet::Edit(edit_changes) => edit_changes.is_empty(),
            ChangeSet::Rearrange(move_tracker) => move_tracker.is_empty(),
        }
    }

    pub fn move_node(&mut self, from: Path, to: Path) -> SessionResult<()> {
        self.move_tracker_mut()?.record(from, to);
        Ok(())
    }

    pub fn moved_to<'a>(&self, path: &'a Path) -> Option<Cow<'a, Path>> {
        self.move_tracker().moved_to(path)
    }

    pub fn moved_from<'a>(&self, path: &'a Path) -> Option<Cow<'a, Path>> {
        self.move_tracker().moved_from(path)
    }

    pub fn add_group(
        &mut self,
        path: Path,
        node_id: NodeId,
        definition: Bytes,
    ) -> SessionResult<()> {
        debug_assert!(!self.edits().updated_groups.contains_key(&node_id));
        self.edits_mut()?.new_groups.insert(path, (node_id, definition));
        Ok(())
    }

    pub fn get_group(&self, path: &Path) -> Option<&(NodeId, Bytes)> {
        self.edits().new_groups.get(path)
    }

    pub fn get_array(&self, path: &Path) -> Option<&(NodeId, ArrayData)> {
        self.edits().new_arrays.get(path)
    }

    /// IMPORTANT: This method does not delete children. The caller
    /// is responsible for doing that
    pub fn delete_group(&mut self, path: Path, node_id: &NodeId) -> SessionResult<()> {
        let edits = self.edits_mut()?;
        edits.updated_groups.remove(node_id);
        if edits.new_groups.remove(&path).is_none() {
            // it's an old group, we need to flag it as deleted
            edits.deleted_groups.insert((path, node_id.clone()));
        }
        Ok(())
    }

    pub fn add_array(
        &mut self,
        path: Path,
        node_id: NodeId,
        array_data: ArrayData,
    ) -> SessionResult<()> {
        self.edits_mut()?.new_arrays.insert(path, (node_id, array_data));
        Ok(())
    }

    pub fn update_array(
        &mut self,
        node_id: &NodeId,
        path: &Path,
        array_data: ArrayData,
        new_splits: &ManifestSplits,
    ) -> SessionResult<()> {
        let edits = self.edits_mut()?;
        match edits.new_arrays.get(path) {
            Some((id, _)) => {
                debug_assert!(!edits.updated_arrays.contains_key(id));
                edits.new_arrays.insert(path.clone(), (node_id.clone(), array_data));
            }
            None => {
                edits.updated_arrays.insert(node_id.clone(), array_data);
            }
        }

        // update existing splits
        let mut to_remove = HashSet::<ChunkIndices>::new();
        if let Some(manifests) = edits.set_chunks.remove(node_id) {
            let mut new_deleted_chunks = HashSet::<ChunkIndices>::new();
            let mut new_manifests =
                HashMap::<ManifestExtents, SplitManifest>::with_capacity(
                    new_splits.len(),
                );
            for (old_extents, mut chunks) in manifests.into_iter() {
                for new_extents in new_splits.iter() {
                    if old_extents.overlap_with(new_extents) == Overlap::None {
                        continue;
                    }

                    // TODO: replace with `BTreeMap.drain_filter` after it is stable.
                    let mut extracted =
                        BTreeMap::<ChunkIndices, Option<ChunkPayload>>::new();
                    chunks.retain(|coord, payload| {
                        let cond = new_extents.contains(coord.0.as_slice());
                        if cond {
                            extracted.insert(coord.clone(), payload.clone());
                        }
                        !cond
                    });
                    new_manifests
                        .entry(new_extents.clone())
                        .or_default()
                        .extend(extracted);
                }
                new_deleted_chunks.extend(
                    chunks.into_iter().filter_map(|(coord, payload)| {
                        payload.is_none().then_some(coord)
                    }),
                );
            }

            // bring back any previously tracked deletes
            if let Some(deletes) = edits.deleted_chunks_outside_bounds.get_mut(node_id) {
                for coord in deletes.iter() {
                    if let Some(extents) = new_splits.find(coord) {
                        new_manifests
                            .entry(extents.clone())
                            .or_default()
                            .insert(coord.clone(), None);
                        to_remove.insert(coord.clone());
                    };
                }
                deletes.retain(|item| !to_remove.contains(item));
                to_remove.drain();
            };
            edits.set_chunks.insert(node_id.clone(), new_manifests);

            // keep track of any deletes not inserted in to set_chunks
            edits
                .deleted_chunks_outside_bounds
                .entry(node_id.clone())
                .or_default()
                .extend(new_deleted_chunks);
        }
        Ok(())
    }

    pub fn update_group(
        &mut self,
        node_id: &NodeId,
        path: &Path,
        definition: Bytes,
    ) -> SessionResult<()> {
        let edits = self.edits_mut()?;
        match edits.new_groups.get(path) {
            Some((id, _)) => {
                debug_assert!(!edits.updated_groups.contains_key(id));
                edits.new_groups.insert(path.clone(), (node_id.clone(), definition));
            }
            None => {
                edits.updated_groups.insert(node_id.clone(), definition);
            }
        }
        Ok(())
    }

    pub fn delete_array(&mut self, path: Path, node_id: &NodeId) -> SessionResult<()> {
        // if deleting a new array created in this session, just remove the entry
        // from new_arrays
        let edits = self.edits_mut()?;
        let node_and_meta = edits.new_arrays.remove(&path);
        let is_new_array = node_and_meta.is_some();
        debug_assert!(
            !is_new_array || node_and_meta.map(|n| n.0).as_ref() == Some(node_id)
        );

        edits.updated_arrays.remove(node_id);
        edits.set_chunks.remove(node_id);
        if !is_new_array {
            edits.deleted_arrays.insert((path, node_id.clone()));
        }
        Ok(())
    }

    pub fn is_deleted(&self, path: &Path, node_id: &NodeId) -> bool {
        let key = (path.clone(), node_id.clone());
        let edits = self.edits();
        edits.deleted_groups.contains(&key) || edits.deleted_arrays.contains(&key)
    }

    //pub fn has_updated_definition(&self, node_id: &NodeId) -> bool {
    //    self.updated_definitions.contains_key(node_id)
    //}

    pub fn get_updated_array(&self, node_id: &NodeId) -> Option<&ArrayData> {
        self.edits().updated_arrays.get(node_id)
    }

    pub fn get_updated_group(&self, node_id: &NodeId) -> Option<&Bytes> {
        self.edits().updated_groups.get(node_id)
    }

    pub fn set_chunk_ref(
        &mut self,
        node_id: NodeId,
        coord: ChunkIndices,
        data: Option<ChunkPayload>,
        splits: &ManifestSplits,
    ) -> SessionResult<()> {
        #[allow(clippy::expect_used)]
        let extent = splits.find(&coord).expect("logic bug. Trying to set chunk ref but can't find the appropriate split manifest.");
        // this implementation makes delete idempotent
        // it allows deleting a deleted chunk by repeatedly setting None.
        let edits = self.edits_mut()?;

        let old = edits
            .set_chunks
            .entry(node_id)
            .or_insert_with(|| {
                HashMap::<
                    ManifestExtents,
                    BTreeMap<ChunkIndices, Option<ChunkPayload>>,
                >::with_capacity(splits.len())
            })
            .entry(extent.clone())
            .or_default()
            .insert(coord, data);

        if old.is_none() {
            edits.num_chunks += 1;
        }

        if edits.num_chunks > NUM_CHUNKS_LIMIT && !edits.excessive_num_chunks_warned {
            warn!(
                "There are more than {NUM_CHUNKS_LIMIT} chunk references being loaded into this commit. This is close to the maximum number of chunk modifications Icechunk supports in a single commit, we recommend to split into smaller commits."
            );

            edits.excessive_num_chunks_warned = true;
        }

        Ok(())
    }

    pub fn get_chunk_ref(
        &self,
        node_id: &NodeId,
        coords: &ChunkIndices,
    ) -> Option<&Option<ChunkPayload>> {
        self.edits().set_chunks.get(node_id).and_then(|node_chunks| {
            find_coord(node_chunks.keys(), coords).and_then(|(_, extent)| {
                node_chunks.get(extent).and_then(|s| s.get(coords))
            })
        })
    }

    /// Drop the updated chunk references for the node.
    /// This will only drop the references for which `predicate` returns true
    pub fn drop_chunk_changes(
        &mut self,
        node_id: &NodeId,
        predicate: impl Fn(&ChunkIndices) -> bool,
    ) -> SessionResult<()> {
        if let Some(changes) = self.edits_mut()?.set_chunks.get_mut(node_id) {
            for split in changes.values_mut() {
                split.retain(|coord, _| !predicate(coord));
            }
        }
        Ok(())
    }

    pub fn deleted_chunks_iterator(
        &self,
        node_id: &NodeId,
    ) -> impl Iterator<Item = &ChunkIndices> {
        match self.edits().deleted_chunks_outside_bounds.get(node_id) {
            Some(deletes) => Either::Right(deletes.iter()),
            None => Either::Left(iter::empty()),
        }
    }

    pub fn array_chunks_iterator(
        &self,
        node_id: &NodeId,
        node_path: &Path,
        extent: ManifestExtents,
    ) -> impl Iterator<Item = (&ChunkIndices, &Option<ChunkPayload>)> + use<'_> {
        if self.is_deleted(node_path, node_id) {
            return Either::Left(iter::empty());
        }
        match self.edits().set_chunks.get(node_id) {
            None => Either::Left(iter::empty()),
            Some(h) => Either::Right(
                h.iter()
                    .filter(move |(manifest_extent, _)| extent.matches(manifest_extent))
                    .flat_map(|(_, manifest)| manifest.iter()),
            ),
        }
    }

    pub fn new_arrays_chunk_iterator(
        &self,
    ) -> impl Iterator<Item = (Path, ChunkInfo)> + use<'_> {
        self.edits().new_arrays.iter().flat_map(|(path, (node_id, _))| {
            self.new_array_chunk_iterator(node_id, path, ManifestExtents::ALL)
                .map(|ci| (path.clone(), ci))
        })
    }

    pub fn new_array_chunk_iterator<'a>(
        &'a self,
        node_id: &'a NodeId,
        node_path: &Path,
        extent: ManifestExtents,
    ) -> impl Iterator<Item = ChunkInfo> + use<'a> {
        self.array_chunks_iterator(node_id, node_path, extent).filter_map(
            move |(coords, payload)| {
                payload.as_ref().map(|p| ChunkInfo {
                    node: node_id.clone(),
                    coord: coords.clone(),
                    payload: p.clone(),
                })
            },
        )
    }

    pub fn modified_manifest_extents_iterator(
        &self,
        node_id: &NodeId,
        node_path: &Path,
    ) -> impl Iterator<Item = &ManifestExtents> + use<'_> {
        if self.is_deleted(node_path, node_id) {
            return Either::Left(iter::empty());
        }
        match self.edits().set_chunks.get(node_id) {
            None => Either::Left(iter::empty()),
            Some(h) => Either::Right(h.keys()),
        }
    }

    pub fn array_manifest(
        &self,
        node_id: &NodeId,
        extent: &ManifestExtents,
    ) -> Option<&SplitManifest> {
        self.edits().set_chunks.get(node_id).and_then(|x| x.get(extent))
    }

    pub fn new_nodes(&self) -> impl Iterator<Item = (&Path, &NodeId)> {
        self.new_groups().chain(self.new_arrays())
    }

    pub fn new_groups(&self) -> impl Iterator<Item = (&Path, &NodeId)> {
        self.edits().new_groups.iter().map(|(path, (node_id, _))| (path, node_id))
    }

    pub fn new_arrays(&self) -> impl Iterator<Item = (&Path, &NodeId)> {
        self.edits().new_arrays.iter().map(|(path, (node_id, _))| (path, node_id))
    }

    /// Merge this ChangeSet with `other`.
    ///
    /// Results of the merge are applied to `self`. Changes present in `other` take precedence over
    /// `self` changes.
    pub fn merge(&mut self, other: ChangeSet) -> SessionResult<()> {
        match (self, other) {
            (ChangeSet::Edit(my_edit_changes), ChangeSet::Edit(other_changes)) => {
                my_edit_changes.merge(other_changes);
                Ok(())
            }
            _ => Err(SessionErrorKind::RearrangeSessionOnly.into()),
        }
    }

    /// Serialize this ChangeSet
    ///
    /// This is intended to help with marshalling distributed writers back to the coordinator
    pub fn export_to_bytes(&self) -> SessionResult<Vec<u8>> {
        Ok(rmp_serde::to_vec(self).map_err(Box::new)?)
    }

    /// Deserialize a ChangeSet
    ///
    /// This is intended to help with marshalling distributed writers back to the coordinator
    pub fn import_from_bytes(bytes: &[u8]) -> SessionResult<Self> {
        Ok(rmp_serde::from_slice(bytes).map_err(Box::new)?)
    }

    pub fn update_existing_chunks<'a, E>(
        &'a self,
        node: NodeId,
        chunks: impl Iterator<Item = Result<ChunkInfo, E>> + 'a,
    ) -> impl Iterator<Item = Result<ChunkInfo, E>> + 'a {
        chunks.filter_map_ok(move |chunk| match self.get_chunk_ref(&node, &chunk.coord) {
            None => Some(chunk),
            Some(new_payload) => {
                new_payload.clone().map(|pl| ChunkInfo { payload: pl, ..chunk })
            }
        })
    }

    pub fn get_new_node(&self, path: &Path) -> Option<NodeSnapshot> {
        self.get_new_array(path).or(self.get_new_group(path))
    }

    pub fn get_new_array(&self, path: &Path) -> Option<NodeSnapshot> {
        self.get_array(path).map(|(id, array_data)| {
            debug_assert!(!self.edits().updated_arrays.contains_key(id));
            NodeSnapshot {
                id: id.clone(),
                path: path.clone(),
                user_data: array_data.user_data.clone(),
                // We put no manifests in new arrays, see get_chunk_ref to understand how chunks get
                // fetched for those arrays
                node_data: NodeData::Array {
                    shape: array_data.shape.clone(),
                    dimension_names: array_data.dimension_names.clone(),
                    manifests: vec![],
                },
            }
        })
    }

    pub fn get_new_group(&self, path: &Path) -> Option<NodeSnapshot> {
        self.get_group(path).map(|(id, definition)| {
            debug_assert!(!self.edits().updated_groups.contains_key(id));
            NodeSnapshot {
                id: id.clone(),
                path: path.clone(),
                user_data: definition.clone(),
                node_data: NodeData::Group,
            }
        })
    }

    pub fn new_nodes_iterator(&self) -> impl Iterator<Item = NodeSnapshot> {
        self.new_nodes().filter_map(move |(path, node_id)| {
            if self.is_deleted(path, node_id) {
                return None;
            }
            // we should be able to create the full node because we
            // know it's a new node
            #[allow(clippy::expect_used)]
            let node = self.get_new_node(path).expect("Bug in new_nodes implementation");
            Some(node)
        })
    }

    // Applies the changeset to an existing node, yielding a new node if it hasn't been deleted
    pub fn update_existing_node(&self, node: NodeSnapshot) -> Option<NodeSnapshot> {
        // we need to take into account moves
        let new_path = self.moved_to(&node.path)?;
        if self.is_deleted(new_path.as_ref(), &node.id) {
            return None;
        }

        let edits = self.edits();
        match node.node_data {
            NodeData::Group => {
                let new_definition =
                    edits.updated_groups.get(&node.id).cloned().unwrap_or(node.user_data);
                Some(NodeSnapshot {
                    user_data: new_definition,
                    path: new_path.into_owned(),
                    ..node
                })
            }
            NodeData::Array { shape, dimension_names, manifests } => {
                let new_data =
                    edits.updated_arrays.get(&node.id).cloned().unwrap_or_else(|| {
                        ArrayData { shape, dimension_names, user_data: node.user_data }
                    });
                Some(NodeSnapshot {
                    user_data: new_data.user_data,
                    node_data: NodeData::Array {
                        shape: new_data.shape,
                        dimension_names: new_data.dimension_names,
                        manifests,
                    },
                    path: new_path.into_owned(),
                    ..node
                })
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;

    use super::ChangeSet;

    use crate::{
        change_set::{ArrayData, EditChanges, MoveTracker},
        format::{
            ChunkIndices, NodeId, Path,
            manifest::{ChunkInfo, ChunkPayload, ManifestSplits},
            snapshot::ArrayShape,
        },
        roundtrip_serialization_tests,
    };

    #[icechunk_macros::test]
    fn test_new_arrays_chunk_iterator() -> Result<(), Box<dyn std::error::Error>> {
        let mut change_set = ChangeSet::Edit(Default::default());
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        let shape = ArrayShape::new(vec![(2, 1), (2, 1), (2, 1)]).unwrap();
        let dimension_names = Some(vec!["x".into(), "y".into(), "t".into()]);

        let node_id1 = NodeId::random();
        let node_id2 = NodeId::random();
        let array_data = ArrayData {
            shape: shape.clone(),
            dimension_names: dimension_names.clone(),
            user_data: Bytes::from_static(b"foobar"),
        };
        change_set.add_array(
            "/foo/bar".try_into().unwrap(),
            node_id1.clone(),
            array_data.clone(),
        )?;
        change_set.add_array(
            "/foo/baz".try_into().unwrap(),
            node_id2.clone(),
            array_data.clone(),
        )?;
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        let splits1 = ManifestSplits::from_edges(vec![vec![0, 10], vec![0, 10]]);

        change_set.set_chunk_ref(
            node_id1.clone(),
            ChunkIndices(vec![0, 1]),
            None,
            &splits1,
        )?;
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        change_set.set_chunk_ref(
            node_id1.clone(),
            ChunkIndices(vec![1, 0]),
            Some(ChunkPayload::Inline("bar1".into())),
            &splits1,
        )?;
        change_set.set_chunk_ref(
            node_id1.clone(),
            ChunkIndices(vec![1, 1]),
            Some(ChunkPayload::Inline("bar2".into())),
            &splits1,
        )?;

        let splits2 = ManifestSplits::from_edges(vec![vec![0, 10]]);
        change_set.set_chunk_ref(
            node_id2.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("baz1".into())),
            &splits2,
        )?;
        change_set.set_chunk_ref(
            node_id2.clone(),
            ChunkIndices(vec![1]),
            Some(ChunkPayload::Inline("baz2".into())),
            &splits2,
        )?;

        {
            let all_chunks: Vec<_> = change_set
                .new_arrays_chunk_iterator()
                .sorted_by_key(|c| c.1.coord.clone())
                .collect();
            let expected_chunks: Vec<_> = [
                (
                    "/foo/baz".try_into().unwrap(),
                    ChunkInfo {
                        node: node_id2.clone(),
                        coord: ChunkIndices(vec![0]),
                        payload: ChunkPayload::Inline("baz1".into()),
                    },
                ),
                (
                    "/foo/baz".try_into().unwrap(),
                    ChunkInfo {
                        node: node_id2.clone(),
                        coord: ChunkIndices(vec![1]),
                        payload: ChunkPayload::Inline("baz2".into()),
                    },
                ),
                (
                    "/foo/bar".try_into().unwrap(),
                    ChunkInfo {
                        node: node_id1.clone(),
                        coord: ChunkIndices(vec![1, 0]),
                        payload: ChunkPayload::Inline("bar1".into()),
                    },
                ),
                (
                    "/foo/bar".try_into().unwrap(),
                    ChunkInfo {
                        node: node_id1.clone(),
                        coord: ChunkIndices(vec![1, 1]),
                        payload: ChunkPayload::Inline("bar2".into()),
                    },
                ),
            ]
            .into();
            assert_eq!(all_chunks, expected_chunks);
        }
        Ok(())
    }

    #[icechunk_macros::test]
    fn test_new_path_for_simple() {
        let mut mt = MoveTracker::default();
        mt.record(Path::new("/foo/bar/old").unwrap(), Path::new("/foo/bar/new").unwrap());
        mt.record(
            Path::new("/foo/bar/new/inner-old1").unwrap(),
            Path::new("/foo/bar/new/inner-new").unwrap(),
        );
        mt.record(
            Path::new("/foo/bar/new/inner-old2").unwrap(),
            Path::new("/inner-new2").unwrap(),
        );

        assert_eq!(
            mt.moved_to(&Path::new("/foo").unwrap()).unwrap().as_ref(),
            &Path::new("/foo").unwrap()
        );
        assert_eq!(
            mt.moved_to(&Path::new("/foo/bar").unwrap()).unwrap().as_ref(),
            &Path::new("/foo/bar").unwrap()
        );
        assert_eq!(
            mt.moved_to(&Path::new("/foo/bar/old").unwrap()).unwrap().as_ref(),
            &Path::new("/foo/bar/new").unwrap()
        );
        assert_eq!(
            mt.moved_to(&Path::new("/foo/bar/old/more").unwrap()).unwrap().as_ref(),
            &Path::new("/foo/bar/new/more").unwrap()
        );
        assert_eq!(
            mt.moved_to(&Path::new("/foo/bar/old/more/andmore").unwrap())
                .unwrap()
                .as_ref(),
            &Path::new("/foo/bar/new/more/andmore").unwrap()
        );
        assert_eq!(
            mt.moved_to(&Path::new("/other").unwrap()).unwrap().as_ref(),
            &Path::new("/other").unwrap()
        );
    }

    #[icechunk_macros::test]
    fn test_moved_from_simple() {
        let mut mt = MoveTracker::default();
        mt.record(Path::new("/foo/bar/old").unwrap(), Path::new("/foo/bar/new").unwrap());
        mt.record(
            Path::new("/foo/bar/new/inner-old1").unwrap(),
            Path::new("/foo/bar/new/inner-new").unwrap(),
        );
        mt.record(
            Path::new("/foo/bar/new/inner-old2").unwrap(),
            Path::new("/inner-new2").unwrap(),
        );

        assert_eq!(
            mt.moved_from(&Path::new("/foo").unwrap()).unwrap().as_ref(),
            &Path::new("/foo").unwrap()
        );
        assert_eq!(
            mt.moved_from(&Path::new("/foo/bar").unwrap()).unwrap().as_ref(),
            &Path::new("/foo/bar").unwrap()
        );
        assert_eq!(
            mt.moved_from(&Path::new("/foo/bar/new").unwrap()).unwrap().as_ref(),
            &Path::new("/foo/bar/old").unwrap()
        );
        assert_eq!(
            mt.moved_from(&Path::new("/foo/bar/new/more").unwrap()).unwrap().as_ref(),
            &Path::new("/foo/bar/old/more").unwrap()
        );
        assert_eq!(
            mt.moved_from(&Path::new("/foo/bar/new/more/andmore").unwrap())
                .unwrap()
                .as_ref(),
            &Path::new("/foo/bar/old/more/andmore").unwrap()
        );
        assert!(mt.moved_from(&Path::new("/foo/bar/old").unwrap()).is_none());
        assert!(mt.moved_from(&Path::new("/foo/bar/old/more").unwrap()).is_none());
        assert!(
            mt.moved_from(&Path::new("/foo/bar/old/more/andmore").unwrap()).is_none()
        );

        assert_eq!(
            mt.moved_from(&Path::new("/foo/bar/new/inner-new").unwrap())
                .unwrap()
                .as_ref(),
            &Path::new("/foo/bar/old/inner-old1").unwrap()
        );
        assert_eq!(
            mt.moved_from(&Path::new("/inner-new2").unwrap()).unwrap().as_ref(),
            &Path::new("/foo/bar/old/inner-old2").unwrap()
        );
    }

    #[icechunk_macros::test]
    fn test_moved_to_back_and_forth() {
        let mut mt = MoveTracker::default();
        mt.record(Path::new("/foo/bar/old").unwrap(), Path::new("/foo/bar/new").unwrap());
        mt.record(Path::new("/foo/bar/new").unwrap(), Path::new("/foo/bar/old").unwrap());
        assert_eq!(
            mt.moved_to(&Path::new("/foo/bar/old/inner").unwrap()).unwrap().as_ref(),
            &Path::new("/foo/bar/old/inner").unwrap(),
        );
        assert_eq!(
            mt.moved_to(&Path::new("/foo/bar/old").unwrap()).unwrap().as_ref(),
            &Path::new("/foo/bar/old").unwrap(),
        );
        assert_eq!(
            mt.moved_to(&Path::new("/other").unwrap()).unwrap().as_ref(),
            &Path::new("/other").unwrap(),
        );
        assert!(mt.moved_to(&Path::new("/foo/bar/new/other").unwrap()).is_none());
    }

    #[icechunk_macros::test]
    fn test_moved_from_back_and_forth() {
        let mut mt = MoveTracker::default();
        mt.record(Path::new("/foo/bar/old").unwrap(), Path::new("/foo/bar/new").unwrap());
        mt.record(Path::new("/foo/bar/new").unwrap(), Path::new("/foo/bar/old").unwrap());
        assert_eq!(
            mt.moved_from(&Path::new("/foo/bar/old/inner").unwrap()).unwrap().as_ref(),
            &Path::new("/foo/bar/old/inner").unwrap(),
        );
        assert_eq!(
            mt.moved_from(&Path::new("/foo/bar/old").unwrap()).unwrap().as_ref(),
            &Path::new("/foo/bar/old").unwrap(),
        );
        assert_eq!(
            mt.moved_from(&Path::new("/other").unwrap()).unwrap().as_ref(),
            &Path::new("/other").unwrap(),
        );
    }

    use crate::strategies::{
        array_data, bytes, gen_move, large_chunk_indices, manifest_extents, node_id,
        path, split_manifest,
    };
    use proptest::collection::{btree_map, hash_map, hash_set, vec};
    use proptest::prelude::*;

    prop_compose! {
        fn edit_changes()(num_of_dims in 1..=20usize)(
            new_groups in hash_map(path(),(node_id(), bytes()), 3..7),
                new_arrays in hash_map(path(),(node_id(), array_data()), 3..7),
           updated_arrays in hash_map(node_id(), array_data(), 3..7),
           updated_groups in hash_map(node_id(), bytes(), 3..7),
           set_chunks in btree_map(node_id(),
                hash_map(manifest_extents(num_of_dims), split_manifest(), 3..7),
            3..7),
    deleted_chunks_outside_bounds in btree_map(node_id(), hash_set(large_chunk_indices(num_of_dims), 3..8), 3..7),
            deleted_groups in hash_set((path(), node_id()), 3..7),
            deleted_arrays in hash_set((path(), node_id()), 3..7)
        ) -> EditChanges {
            EditChanges{new_groups, updated_groups, updated_arrays, set_chunks, num_chunks: 0, excessive_num_chunks_warned: false, deleted_chunks_outside_bounds, deleted_arrays, deleted_groups, new_arrays}
        }
    }

    fn move_tracker() -> impl Strategy<Value = MoveTracker> {
        vec(gen_move(), 1..5).prop_map(MoveTracker).boxed()
    }

    fn change_set() -> impl Strategy<Value = ChangeSet> {
        use ChangeSet::*;
        prop_oneof![edit_changes().prop_map(Edit), move_tracker().prop_map(Rearrange)]
            .boxed()
    }

    roundtrip_serialization_tests!(serialize_and_deserialize_change_sets - change_set);
}
