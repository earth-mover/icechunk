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
        manifest::{ChunkInfo, ChunkPayload},
        snapshot::{ArrayShape, DimensionName, NodeData, NodeSnapshot},
    },
    session::{SessionError, SessionErrorKind, SessionResult},
};
use icechunk_types::ICResultExt as _;

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

pub(crate) type ChunkTable = BTreeMap<ChunkIndices, Option<ChunkPayload>>;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EditChanges {
    new_groups: HashMap<Path, (NodeId, Bytes)>,
    new_arrays: HashMap<Path, (NodeId, ArrayData)>,
    updated_arrays: HashMap<NodeId, ArrayData>,
    updated_groups: HashMap<NodeId, Bytes>,
    set_chunks: BTreeMap<NodeId, ChunkTable>,

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

        other.set_chunks.into_iter().for_each(|(node, other_manifest)| {
            match self.set_chunks.get_mut(&node) {
                Some(manifest) => manifest.extend(other_manifest),
                None => {
                    self.set_chunks.insert(node, other_manifest);
                }
            }
        });
    }
}

pub static EMPTY_EDITS: LazyLock<EditChanges> = LazyLock::new(Default::default);

pub use icechunk_types::Move;

#[derive(Debug, PartialEq)]
pub enum MovedTo<'a> {
    To(Cow<'a, Path>),
    NotMoved(Cow<'a, Path>),
    Overwritten,
}

#[derive(Debug, PartialEq)]
pub enum MovedFrom<'a> {
    From(Cow<'a, Path>),
    NotMoved(Cow<'a, Path>),
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MoveTracker {
    moves: Vec<Move>,
    #[serde(default)]
    nodes_by_original: HashMap<Path, Path>,
    #[serde(default)]
    nodes_by_final: BTreeMap<Path, Path>,
}

pub static EMPTY_MOVE_TRACKER: LazyLock<MoveTracker> = LazyLock::new(Default::default);

impl MoveTracker {
    /// Record a move and update the node maps with all affected nodes.
    ///
    /// `subtree_nodes` contains the original paths of all nodes under
    /// the source. These are used to populate the node maps.
    ///
    /// Example: tree `/a/b/c`, move `/a` -> `/x`
    ///   `subtree_nodes` = `[/a, /a/b, /a/b/c]`
    ///   After: `nodes_by_original` = `{/a: /x, /a/b: /x/b, /a/b/c: /x/b/c}`
    pub fn record(
        &mut self,
        from: Path,
        to: Path,
        subtree_nodes: impl IntoIterator<Item = Path>,
    ) {
        // Resolve `from` to its original snapshot path — it may have
        // been renamed by an earlier move. O(log N) BTreeMap lookup.
        // QUESTION: should we just do moved_from here? maybe faster?
        let original_from =
            self.nodes_by_final.get(&from).cloned().unwrap_or_else(|| from.clone());
        // Step 1: Update existing map entries whose current path is
        // under `from` — they get remapped to `to`.
        // e.g. earlier move deposited /x at /a/x, now /a -> /c:
        //   /x's current path /a/x starts_with /a -> becomes /c/x
        //
        // Collect updates first since we can't mutate BTreeMap keys
        // while iterating.
        let mut updates: Vec<(Path, Path, Path)> = Vec::new(); // (original, old_final, new_final)
        for (orig, current) in self.nodes_by_original.iter_mut() {
            if let Some(remapped) = Self::remap_path(current, &from, &to) {
                updates.push((orig.clone(), current.clone(), remapped.clone()));
                *current = remapped;
            }
        }
        for (orig, old_final, new_final) in updates {
            self.nodes_by_final.remove(&old_final);
            self.nodes_by_final.insert(new_final, orig);
        }

        // Step 2: Add entries for nodes not yet in the map.
        // These are nodes that haven't been touched by any prior move.
        for orig in subtree_nodes {
            if !self.nodes_by_original.contains_key(&orig)
                && let Some(new_path) = Self::remap_path(&orig, &original_from, &to)
            {
                self.nodes_by_final.insert(new_path.clone(), orig.clone());
                self.nodes_by_original.insert(orig, new_path);
            }
        }

        self.moves.push(Move { from, to });
    }

    /// Return `(original_path, final_path)` pairs for all nodes whose
    /// final path is under `parent_group`. Uses a `BTreeMap` range scan
    /// for efficient prefix queries on large node sets.
    pub fn moved_into(&self, parent_group: &Path) -> Vec<(Path, Path)> {
        self.nodes_by_final
            .range(parent_group.clone()..)
            .take_while(|(final_path, _)| final_path.starts_with(parent_group))
            .map(|(final_path, orig)| (orig.clone(), final_path.clone()))
            .collect()
    }

    /// Check if a node's original path has been remapped by a move.
    pub fn is_remapped(&self, original_path: &Path) -> bool {
        self.nodes_by_original.contains_key(original_path)
    }

    pub fn is_empty(&self) -> bool {
        self.moves.is_empty()
    }

    pub fn all_moves(&self) -> impl Iterator<Item = &Move> {
        self.moves.iter()
    }

    /// Remap `path` by replacing `old_prefix` with `new_prefix`.
    /// Returns None if `path` is not under `old_prefix`.
    fn remap_path(path: &Path, old_prefix: &Path, new_prefix: &Path) -> Option<Path> {
        path.buf().strip_prefix(old_prefix.buf()).ok().map(|rest| {
            #[expect(clippy::expect_used)]
            Path::new(new_prefix.buf().join(rest).to_string().as_str())
                .expect("Bug in remap_path, cannot create path")
        })
    }

    pub fn moved_to<'a>(&self, path: &'a Path) -> MovedTo<'a> {
        let mut res = Cow::Borrowed(path);
        let mut was_moved = false;
        for Move { from, to } in self.moves.iter() {
            if let Some(new_path) = Self::remap_path(res.as_ref(), from, to) {
                res = Cow::Owned(new_path);
                was_moved = true;
            } else if res.buf().starts_with(to.buf()) {
                // the path has been overwritten by the moves
                // calling code should check for overwrites before moving
                return MovedTo::Overwritten;
            }
        }
        if was_moved { MovedTo::To(res) } else { MovedTo::NotMoved(res) }
    }

    pub fn moved_from<'a>(&self, path: &'a Path) -> MovedFrom<'a> {
        let mut res = Cow::Borrowed(path);
        let mut was_moved = false;
        for Move { from, to } in self.moves.iter().rev() {
            if let Some(old_path) = Self::remap_path(res.as_ref(), to, from) {
                res = Cow::Owned(old_path);
                was_moved = true;
            } else if res.buf().starts_with(from.buf()) {
                // the moves have deleted this path
                // calling code should check for overwrites before moving
                return MovedFrom::Deleted;
            }
        }
        if was_moved { MovedFrom::From(res) } else { MovedFrom::NotMoved(res) }
    }
}

/// Uncommitted modifications accumulated during a session.
///
/// Two variants for the two writable session modes:
/// - [`Edit`](ChangeSet::Edit) - New/updated/deleted arrays, groups, and chunks
/// - [`Rearrange`](ChangeSet::Rearrange) - Move/rename operations
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
// we only keep one of this, their size difference doesn't affect us
#[expect(clippy::large_enum_variant)]
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
            ChangeSet::Rearrange(_) => {
                Err(SessionError::capture(SessionErrorKind::RearrangeSessionOnly))
            }
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
            ChangeSet::Edit(_) => {
                Err(SessionError::capture(SessionErrorKind::NonRearrangeSession))
            }
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
        self.edits()
            .set_chunks
            .iter()
            .map(|(node_id, manifest)| (node_id, manifest.keys()))
    }

    pub fn changed_node_chunks(
        &self,
        node_id: &NodeId,
    ) -> impl Iterator<Item = &ChunkIndices> {
        match self.edits().set_chunks.get(node_id) {
            Some(chunks) => Either::Left(chunks.keys()),
            None => Either::Right(iter::empty()),
        }
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

    pub fn move_node(
        &mut self,
        from: Path,
        to: Path,
        subtree_nodes: impl IntoIterator<Item = Path>,
    ) -> SessionResult<()> {
        self.move_tracker_mut()?.record(from, to, subtree_nodes);
        Ok(())
    }

    /// Return `(original_path, final_path)` pairs for all nodes whose
    /// final path is under `parent_group`.
    pub fn moved_into(&self, parent_group: &Path) -> Vec<(Path, Path)> {
        self.move_tracker().moved_into(parent_group)
    }

    /// Check if a node's original path has been remapped by a move.
    pub fn is_remapped(&self, original_path: &Path) -> bool {
        self.move_tracker().is_remapped(original_path)
    }

    pub fn moved_to<'a>(&self, path: &'a Path) -> MovedTo<'a> {
        self.move_tracker().moved_to(path)
    }

    pub fn moved_from<'a>(&self, path: &'a Path) -> MovedFrom<'a> {
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
    ) -> SessionResult<()> {
        // this implementation makes delete idempotent
        // it allows deleting a deleted chunk by repeatedly setting None.
        let edits = self.edits_mut()?;
        let old = edits.set_chunks.entry(node_id).or_default().insert(coord, data);

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
        self.edits()
            .set_chunks
            .get(node_id)
            .and_then(|node_chunks| node_chunks.get(coords))
    }

    /// Drop the updated chunk references for the node.
    /// This will only drop the references for which `predicate` returns true
    pub fn drop_chunk_changes(
        &mut self,
        node_id: &NodeId,
        predicate: impl Fn(&ChunkIndices) -> bool,
    ) -> SessionResult<()> {
        if let Some(changes) = self.edits_mut()?.set_chunks.get_mut(node_id) {
            changes.retain(|coord, _| !predicate(coord));
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
    ) -> impl Iterator<Item = (&ChunkIndices, &Option<ChunkPayload>)> + use<'_> {
        if self.is_deleted(node_path, node_id) {
            return Either::Left(iter::empty());
        }
        match self.edits().set_chunks.get(node_id) {
            None => Either::Left(iter::empty()),
            Some(manifest) => Either::Right(manifest.iter()),
        }
    }

    pub fn new_arrays_chunk_iterator(
        &self,
    ) -> impl Iterator<Item = (Path, ChunkInfo)> + use<'_> {
        self.edits().new_arrays.iter().flat_map(|(path, (node_id, _))| {
            self.array_chunks_iterator(node_id, path).filter_map(
                move |(coords, payload)| {
                    payload.as_ref().map(|p| {
                        (
                            path.clone(),
                            ChunkInfo {
                                node: node_id.clone(),
                                coord: coords.clone(),
                                payload: p.clone(),
                            },
                        )
                    })
                },
            )
        })
    }

    pub fn array_manifest(&self, node_id: &NodeId) -> Option<&ChunkTable> {
        self.edits().set_chunks.get(node_id)
    }

    pub fn new_nodes(&self) -> impl Iterator<Item = (&Path, &NodeId)> {
        self.new_groups().chain(self.new_arrays().map(|(path, id, _)| (path, id)))
    }

    pub fn new_groups(&self) -> impl Iterator<Item = (&Path, &NodeId)> {
        self.edits().new_groups.iter().map(|(path, (node_id, _))| (path, node_id))
    }

    pub fn new_arrays(&self) -> impl Iterator<Item = (&Path, &NodeId, &ArrayData)> {
        self.edits()
            .new_arrays
            .iter()
            .map(|(path, (node_id, node_data))| (path, node_id, node_data))
    }

    /// Merge this `ChangeSet` with `other`.
    ///
    /// Results of the merge are applied to `self`. Changes present in `other` take precedence over
    /// `self` changes.
    pub fn merge(&mut self, other: ChangeSet) -> SessionResult<()> {
        match (self, other) {
            (ChangeSet::Edit(my_edit_changes), ChangeSet::Edit(other_changes)) => {
                my_edit_changes.merge(other_changes);
                Ok(())
            }
            _ => Err(SessionError::capture(SessionErrorKind::RearrangeSessionOnly)),
        }
    }

    /// Serialize this `ChangeSet`
    ///
    /// This is intended to help with marshalling distributed writers back to the coordinator
    pub fn export_to_bytes(&self) -> SessionResult<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(Box::new).capture()
    }

    /// Deserialize a `ChangeSet`
    ///
    /// This is intended to help with marshalling distributed writers back to the coordinator
    pub fn import_from_bytes(bytes: &[u8]) -> SessionResult<Self> {
        rmp_serde::from_slice(bytes).map_err(Box::new).capture()
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
            #[expect(clippy::expect_used)]
            let node = self.get_new_node(path).expect("Bug in new_nodes implementation");
            Some(node)
        })
    }

    // Applies the changeset to an existing node, yielding a new node if it hasn't been deleted
    pub fn update_existing_node(&self, node: NodeSnapshot) -> Option<NodeSnapshot> {
        // we need to take into account moves
        let new_path = {
            use MovedTo::*;
            match self.moved_to(&node.path) {
                Overwritten => return None,
                NotMoved(cow) | To(cow) => cow,
            }
        };
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

pub fn transaction_log_from_change_set(
    id: &crate::format::SnapshotId,
    cs: &ChangeSet,
) -> crate::format::transaction_log::TransactionLog {
    use crate::format::transaction_log::TransactionLog;

    let mut new_groups: Vec<_> = cs.new_groups().map(|(_, id)| id.clone()).collect();
    let mut new_arrays: Vec<_> = cs.new_arrays().map(|(_, id, _)| id.clone()).collect();
    let mut deleted_groups: Vec<_> =
        cs.deleted_groups().map(|(_, id)| id.clone()).collect();
    let mut deleted_arrays: Vec<_> =
        cs.deleted_arrays().map(|(_, id)| id.clone()).collect();
    let mut updated_arrays: Vec<_> = cs.updated_arrays().cloned().collect();
    let mut updated_groups: Vec<_> = cs.updated_groups().cloned().collect();

    new_groups.sort();
    new_arrays.sort();
    deleted_groups.sort();
    deleted_arrays.sort();
    updated_arrays.sort();
    updated_groups.sort();

    let changed_chunks: Vec<_> = cs
        .changed_chunks()
        .map(|(node_id, chunks)| {
            (node_id.clone(), chunks.cloned().collect::<Vec<_>>().into_iter())
        })
        .collect();

    TransactionLog::new_from_parts(
        id,
        new_groups.into_iter(),
        new_arrays.into_iter(),
        deleted_groups.into_iter(),
        deleted_arrays.into_iter(),
        updated_groups.into_iter(),
        updated_arrays.into_iter(),
        changed_chunks.into_iter(),
        cs.moves().cloned(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use bytes::Bytes;
    use itertools::Itertools as _;

    /// Test helper: record a move without subtree nodes.
    fn rec(mt: &mut MoveTracker, from: &str, to: &str) {
        let from = Path::new(from).unwrap();
        let to = Path::new(to).unwrap();
        mt.record(from, to, std::iter::empty());
    }

    use super::ChangeSet;

    use crate::{
        change_set::{ArrayData, EditChanges, MoveTracker},
        format::{
            ChunkIndices, NodeId, Path,
            manifest::{ChunkInfo, ChunkPayload},
            snapshot::ArrayShape,
        },
    };
    use icechunk_format::roundtrip_serialization_tests;

    #[icechunk_macros::test]
    fn test_new_arrays_chunk_iterator() -> Result<(), Box<dyn std::error::Error>> {
        let mut change_set = ChangeSet::Edit(Default::default());
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        let shape = ArrayShape::new(vec![(2, 2), (2, 2), (2, 2)]).unwrap();
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

        change_set.set_chunk_ref(node_id1.clone(), ChunkIndices(vec![0, 1]), None)?;
        assert_eq!(None, change_set.new_arrays_chunk_iterator().next());

        change_set.set_chunk_ref(
            node_id1.clone(),
            ChunkIndices(vec![1, 0]),
            Some(ChunkPayload::Inline("bar1".into())),
        )?;
        change_set.set_chunk_ref(
            node_id1.clone(),
            ChunkIndices(vec![1, 1]),
            Some(ChunkPayload::Inline("bar2".into())),
        )?;

        change_set.set_chunk_ref(
            node_id2.clone(),
            ChunkIndices(vec![0]),
            Some(ChunkPayload::Inline("baz1".into())),
        )?;
        change_set.set_chunk_ref(
            node_id2.clone(),
            ChunkIndices(vec![1]),
            Some(ChunkPayload::Inline("baz2".into())),
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
        use super::MovedTo::*;

        let mut mt = MoveTracker::default();
        rec(&mut mt, "/foo/bar/old", "/foo/bar/new");
        rec(&mut mt, "/foo/bar/new/inner-old1", "/foo/bar/new/inner-new");
        rec(&mut mt, "/foo/bar/new/inner-old2", "/inner-new2");

        assert!(matches!(
            mt.moved_to(&Path::new("/foo").unwrap()),
            NotMoved(p) if p.as_ref() == &Path::new("/foo").unwrap()
        ));
        assert!(matches!(
            mt.moved_to(&Path::new("/foo/bar").unwrap()),
            NotMoved(p) if p.as_ref() == &Path::new("/foo/bar").unwrap()
        ));
        assert!(matches!(
            mt.moved_to(&Path::new("/foo/bar/old").unwrap()),
            To(p) if p.as_ref() == &Path::new("/foo/bar/new").unwrap()
        ));
        assert!(matches!(
            mt.moved_to(&Path::new("/foo/bar/old/more").unwrap()),
            To(p) if p.as_ref() == &Path::new("/foo/bar/new/more").unwrap()
        ));
        assert!(matches!(
            mt.moved_to(&Path::new("/foo/bar/old/more/andmore").unwrap()),
            To(p) if p.as_ref() == &Path::new("/foo/bar/new/more/andmore").unwrap()
        ));
        assert!(matches!(
            mt.moved_to(&Path::new("/other").unwrap()),
            NotMoved(p) if p.as_ref() == &Path::new("/other").unwrap()
        ));
    }

    #[icechunk_macros::test]
    fn test_moved_from_simple() {
        use super::MovedFrom::*;
        let mut mt = MoveTracker::default();
        rec(&mut mt, "/foo/bar/old", "/foo/bar/new");
        rec(&mut mt, "/foo/bar/new/inner-old1", "/foo/bar/new/inner-new");
        rec(&mut mt, "/foo/bar/new/inner-old2", "/inner-new2");

        assert!(matches!(
            mt.moved_from(&Path::new("/foo").unwrap()),
            NotMoved(p) if p.as_ref() == &Path::new("/foo").unwrap()
        ));
        assert!(matches!(
            mt.moved_from(&Path::new("/foo/bar").unwrap()),
            NotMoved(p) if p.as_ref() == &Path::new("/foo/bar").unwrap()
        ));
        assert!(matches!(
            mt.moved_from(&Path::new("/foo/bar/new").unwrap()),
            From(p) if p.as_ref() == &Path::new("/foo/bar/old").unwrap()
        ));
        assert!(matches!(
            mt.moved_from(&Path::new("/foo/bar/new/more").unwrap()),
            From(p) if p.as_ref() == &Path::new("/foo/bar/old/more").unwrap()
        ));
        assert!(matches!(
            mt.moved_from(&Path::new("/foo/bar/new/more/andmore").unwrap()),
            From(p) if p.as_ref() == &Path::new("/foo/bar/old/more/andmore").unwrap()
        ));
        assert!(matches!(mt.moved_from(&Path::new("/foo/bar/old").unwrap()), Deleted));
        assert!(matches!(
            mt.moved_from(&Path::new("/foo/bar/old/more").unwrap()),
            Deleted
        ));
        assert!(matches!(
            mt.moved_from(&Path::new("/foo/bar/old/more/andmore").unwrap()),
            Deleted
        ));

        assert!(matches!(
            mt.moved_from(&Path::new("/foo/bar/new/inner-new").unwrap()),
            From(p) if p.as_ref() == &Path::new("/foo/bar/old/inner-old1").unwrap()
        ));
        assert!(matches!(
            mt.moved_from(&Path::new("/inner-new2").unwrap()),
            From(p) if p.as_ref() == &Path::new("/foo/bar/old/inner-old2").unwrap()
        ));
    }

    #[icechunk_macros::test]
    fn test_moved_to_back_and_forth() {
        use super::MovedTo::*;
        let mut mt = MoveTracker::default();
        rec(&mut mt, "/foo/bar/old", "/foo/bar/new");
        rec(&mut mt, "/foo/bar/new", "/foo/bar/old");
        assert!(matches!(
            mt.moved_to(&Path::new("/foo/bar/old/inner").unwrap()),
            To(p) if p.as_ref() == &Path::new("/foo/bar/old/inner").unwrap()
        ));
        assert!(matches!(
            mt.moved_to(&Path::new("/foo/bar/old").unwrap()),
            To(p) if p.as_ref() == &Path::new("/foo/bar/old").unwrap()
        ));
        assert!(matches!(
            mt.moved_to(&Path::new("/other").unwrap()),
            NotMoved(p) if p.as_ref() == &Path::new("/other").unwrap()
        ));
        assert!(matches!(
            mt.moved_to(&Path::new("/foo/bar/new/other").unwrap()),
            Overwritten
        ));
    }

    #[icechunk_macros::test]
    fn test_moved_from_back_and_forth() {
        use super::MovedFrom::*;
        let mut mt = MoveTracker::default();
        rec(&mut mt, "/foo/bar/old", "/foo/bar/new");
        rec(&mut mt, "/foo/bar/new", "/foo/bar/old");
        assert!(matches!(
            mt.moved_from(&Path::new("/foo/bar/old/inner").unwrap()),
            From(p) if p.as_ref() == &Path::new("/foo/bar/old/inner").unwrap()
        ));
        assert!(matches!(
            mt.moved_from(&Path::new("/foo/bar/old").unwrap()),
            From(p) if p.as_ref() == &Path::new("/foo/bar/old").unwrap()
        ));
        assert!(matches!(
            mt.moved_from(&Path::new("/other").unwrap()),
            NotMoved(p) if p.as_ref() == &Path::new("/other").unwrap()
        ));
    }

    use crate::strategies::{
        array_data, bytes, gen_move, large_chunk_indices, node_id, path, split_manifest,
    };
    use proptest::collection::{btree_map, hash_map, hash_set, vec};
    use proptest::prelude::*;

    prop_compose! {
        fn edit_changes()(num_of_dims in 1..=5usize)(
            new_groups in hash_map(path(),(node_id(), bytes()), 0..3),
                new_arrays in hash_map(path(),(node_id(), array_data()), 0..3),
           updated_arrays in hash_map(node_id(), array_data(), 0..3),
           updated_groups in hash_map(node_id(), bytes(), 0..3),
           set_chunks in btree_map(node_id(), split_manifest(), 0..3),
            deleted_chunks_outside_bounds in btree_map(node_id(), hash_set(large_chunk_indices(num_of_dims), 0..3), 0..3),
            deleted_groups in hash_set((path(), node_id()), 0..3),
            deleted_arrays in hash_set((path(), node_id()), 0..3)
        ) -> EditChanges {
            EditChanges{new_groups, updated_groups, updated_arrays, set_chunks, num_chunks: 0, excessive_num_chunks_warned: false, deleted_chunks_outside_bounds, deleted_arrays, deleted_groups, new_arrays}
        }
    }

    fn move_tracker() -> impl Strategy<Value = MoveTracker> {
        vec(gen_move(), 1..5)
            .prop_map(|moves| MoveTracker {
                moves,
                nodes_by_original: HashMap::new(),
                nodes_by_final: BTreeMap::new(),
            })
            .boxed()
    }

    fn change_set() -> impl Strategy<Value = ChangeSet> {
        use ChangeSet::*;
        prop_oneof![edit_changes().prop_map(Edit), move_tracker().prop_map(Rearrange)]
            .boxed()
    }

    roundtrip_serialization_tests!(serialize_and_deserialize_change_sets - change_set);

    fn p(s: &str) -> Path {
        Path::new(s).unwrap()
    }

    /// Helper: record a move with explicit subtree nodes.
    fn rec_with(mt: &mut MoveTracker, from: &str, to: &str, subtree: &[&str]) {
        mt.record(p(from), p(to), subtree.iter().map(|s| p(s)));
    }

    #[icechunk_macros::test]
    fn test_node_map_simple_rename() {
        // Tree: /a (group), /a/x (array)
        // Move: /a -> /b
        let mut mt = MoveTracker::default();
        rec_with(&mut mt, "/a", "/b", &["/a", "/a/x"]);

        assert!(mt.is_remapped(&p("/a")));
        assert!(mt.is_remapped(&p("/a/x")));
        assert!(!mt.is_remapped(&p("/b")));

        let under_b = mt.moved_into(&p("/b"));
        assert_eq!(under_b.len(), 2);
        assert!(under_b.contains(&(p("/a"), p("/b"))));
        assert!(under_b.contains(&(p("/a/x"), p("/b/x"))));

        // Listing root returns all moved nodes
        let under_root = mt.moved_into(&Path::root());
        assert_eq!(under_root.len(), 2);
    }

    #[icechunk_macros::test]
    fn test_node_map_deposit_then_rename() {
        // Tree: /a (group), /a/x (array), /b (group), /b/y (array)
        // Move 1: /a -> /b/a (deposit /a into /b)
        // Move 2: /b -> /c (rename /b)
        // After: /c, /c/a, /c/a/x, /c/y
        let mut mt = MoveTracker::default();
        rec_with(&mut mt, "/a", "/b/a", &["/a", "/a/x"]);
        rec_with(&mut mt, "/b", "/c", &["/b", "/b/y"]);

        let result = mt.moved_into(&p("/c"));
        assert!(result.contains(&(p("/a"), p("/c/a"))));
        assert!(result.contains(&(p("/a/x"), p("/c/a/x"))));
        assert!(result.contains(&(p("/b"), p("/c"))));
        assert!(result.contains(&(p("/b/y"), p("/c/y"))));
    }

    #[icechunk_macros::test]
    fn test_node_map_child_moved_out() {
        // Tree: /a (group), /a/x (array), /a/y (array)
        // Move 1: /a/x -> /b (move x out of a)
        // Move 2: /a -> /c (rename a)
        // After: /b (was /a/x), /c (was /a), /c/y (was /a/y)
        let mut mt = MoveTracker::default();
        rec_with(&mut mt, "/a/x", "/b", &["/a/x"]);
        rec_with(&mut mt, "/a", "/c", &["/a", "/a/x", "/a/y"]);

        // /a/x was moved to /b, not carried along to /c
        let under_c = mt.moved_into(&p("/c"));
        assert!(under_c.contains(&(p("/a"), p("/c"))));
        assert!(under_c.contains(&(p("/a/y"), p("/c/y"))));
        assert!(!under_c.iter().any(|(_, f)| *f == p("/c/x")));

        assert!(mt.moved_into(&p("/b")).contains(&(p("/a/x"), p("/b"))));
    }
}
