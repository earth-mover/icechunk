//! Tracks uncommitted modifications during a session.
//!
//! Key types:
//! - [`ChangeSet`] - Enum with variants for the two writable session modes
//! - [`ArrayData`] - Array metadata (shape, dimension names, user attributes)

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    fmt::Display,
    hash::Hash,
    iter, mem,
    sync::LazyLock,
};

use bytes::Bytes;
use itertools::{Either, Itertools as _};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{
    format::{
        ChunkIndices, NodeId, NodeType, Path,
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

    fn check_for_conflicts<K: Display + Hash + Eq, V: PartialEq>(
        a: &HashMap<K, V>,
        b: &HashMap<K, V>,
        reason: &str,
        feature: &str,
    ) -> SessionResult<()> {
        let shared: Vec<_> = a
            .iter()
            .filter_map(|(k, v)| {
                let theirs = b.get(k);
                if theirs.is_none() || Some(v) == theirs { None } else { Some(k) }
            })
            .collect();
        if !shared.is_empty() {
            return Err(SessionError::capture(SessionErrorKind::SessionMerge(format!(
                "Multiple writers {reason} the same {feature}: {}",
                shared.into_iter().join(", ")
            ))));
        }

        Ok(())
    }

    fn merge(&mut self, other: EditChanges) -> SessionResult<()> {
        // TODO: the conflict detection is not comprehensive

        // check if both created same group with different metadata
        Self::check_for_conflicts(
            &self.new_groups,
            &other.new_groups,
            "created",
            "groups",
        )?;

        // check if both updated same group with different metadata
        Self::check_for_conflicts(
            &self.updated_groups,
            &other.updated_groups,
            "updated",
            "groups",
        )?;

        // check if both create same array with different metadata
        Self::check_for_conflicts(
            &self.new_arrays,
            &other.new_arrays,
            "created",
            "arrays",
        )?;

        // check if both updated same array with different metadata
        Self::check_for_conflicts(
            &self.updated_arrays,
            &other.updated_arrays,
            "updated",
            "arrays",
        )?;

        let check_deleted_and_updated_arrays = |a: &Self, b: &Self| {
            let shared: Vec<_> = a
                .deleted_arrays
                .iter()
                .filter_map(|(path, node_id)| {
                    if b.new_arrays.contains_key(path)
                        || b.updated_arrays.contains_key(node_id)
                        || b.set_chunks.contains_key(node_id)
                        || b.deleted_chunks_outside_bounds.contains_key(node_id)
                    {
                        Some(path)
                    } else {
                        None
                    }
                })
                .collect();
            if !shared.is_empty() {
                return Err(SessionError::capture(SessionErrorKind::SessionMerge(
                    format!(
                        "Arrays were both deleted and modified: {}",
                        shared.into_iter().join(", ")
                    ),
                )));
            }
            Ok(())
        };

        let check_deleted_and_updated_groups = |a: &Self, b: &Self| {
            let shared: Vec<_> = a
                .deleted_groups
                .iter()
                .filter_map(|(path, node_id)| {
                    if b.new_groups.contains_key(path)
                        || b.updated_groups.contains_key(node_id)
                    {
                        Some(path)
                    } else {
                        None
                    }
                })
                .collect();
            if !shared.is_empty() {
                return Err(SessionError::capture(SessionErrorKind::SessionMerge(
                    format!(
                        "Groups were both deleted and modified: {}",
                        shared.into_iter().join(", ")
                    ),
                )));
            }
            Ok(())
        };

        // check if a deleted array was modified
        check_deleted_and_updated_arrays(self, &other)?;
        check_deleted_and_updated_arrays(&other, self)?;

        // check if a deleted group was modified
        check_deleted_and_updated_groups(self, &other)?;
        check_deleted_and_updated_groups(&other, self)?;

        // TODO: optimize
        self.new_groups.extend(other.new_groups);
        self.new_arrays.extend(other.new_arrays);
        self.updated_groups.extend(other.updated_groups);
        self.updated_arrays.extend(other.updated_arrays);
        self.deleted_groups.extend(other.deleted_groups);
        self.deleted_arrays.extend(other.deleted_arrays);
        // FIXME: do we even test this?
        self.deleted_chunks_outside_bounds.extend(other.deleted_chunks_outside_bounds);

        // TODO: check conflicts on nodes, for now it is last one wins semantics
        other.set_chunks.into_iter().for_each(|(node, other_manifest)| {
            match self.set_chunks.get_mut(&node) {
                Some(manifest) => manifest.extend(other_manifest),
                None => {
                    self.set_chunks.insert(node, other_manifest);
                }
            }
        });
        Ok(())
    }
}

pub static EMPTY_EDITS: LazyLock<EditChanges> = LazyLock::new(Default::default);

pub use icechunk_format::Move;

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
    nodes_by_original: HashMap<Path, (Path, NodeId, NodeType)>,
    nodes_by_final: BTreeMap<Path, Path>,
}

pub static EMPTY_MOVE_TRACKER: LazyLock<MoveTracker> = LazyLock::new(Default::default);

impl MoveTracker {
    /// Record a move and update the node maps with all affected nodes.
    ///
    /// Example: tree with `/a`, `/a/b`, `/a/b/c`; move `/a` -> `/x`:
    /// ```ignore
    /// tracker.record(path("/a"), path("/x"), vec![path("/a"), path("/a/b"), path("/a/b/c")]);
    /// // moved_into("/x") now returns [(/a, /x), (/a/b, /x/b), (/a/b/c, /x/b/c)]
    /// ```
    pub fn record(
        &mut self,
        from: Path,
        to: Path,
        subtree_nodes: impl IntoIterator<Item = (Path, NodeId, NodeType)>,
        node_id: &NodeId,
        node_type: NodeType,
    ) {
        // Resolve `from` to its original snapshot path — it may have
        // been renamed by an earlier move.
        // QUESTION: should we just do moved_from here? maybe faster?
        let original_from =
            self.nodes_by_final.get(&from).cloned().unwrap_or_else(|| from.clone());

        // Step 1: Update existing map entries whose current final location is
        // under `from` — they get remapped to `to`.
        // e.g. earlier move deposited /x at /a/x, now /a -> /c:
        //   /x's current path /a/x starts_with /a -> becomes /c/x
        //
        // Collect updates first since we can't mutate BTreeMap keys
        // while iterating.
        let mut updates: Vec<(Path, Path, Path)> = Vec::new(); // (original, prev_path, new_path)
        for (original, (current, _node_id, _node_type)) in
            self.nodes_by_original.iter_mut()
        {
            if let Some(remapped) = Self::remap_path(current, &from, &to) {
                let prev_path = mem::replace(current, remapped);
                updates.push((original.clone(), prev_path, current.clone()));
            }
        }

        // Step 2: Add entries for nodes not already in the map.
        // subtree_nodes can contain paths in different forms — original
        // (snapshot) paths or current paths after earlier moves. We check
        // both maps to avoid adding duplicates.
        for (path, node_id, node_type) in subtree_nodes {
            if self.nodes_by_original.contains_key(&path)
                || self.nodes_by_final.contains_key(&path)
            {
                continue;
            }
            if let Some(new_path) = Self::remap_path(&path, &original_from, &to) {
                self.nodes_by_final.insert(new_path.clone(), path.clone());
                self.nodes_by_original.insert(path, (new_path, node_id, node_type));
            }
        }

        // Now update nodes_by_final for carried nodes. No contention with
        // step 2's inserts — step 2 skips anything already in nodes_by_final,
        // and carried nodes operate on disjoint keys.
        for (original, prev_path, new_path) in updates {
            self.nodes_by_final.remove(&prev_path);
            self.nodes_by_final.insert(new_path, original);
        }

        self.moves.push(Move { from, to, node_id: node_id.clone(), node_type });
    }

    /// Return `(original_path, final_path)` pairs for all nodes whose
    /// final path is under `parent_group`.
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
        for Move { from, to, .. } in self.moves.iter() {
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
        for Move { from, to, .. } in self.moves.iter().rev() {
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
        subtree_nodes: impl IntoIterator<Item = (Path, NodeId, NodeType)>,
        node_id: &NodeId,
        node_type: NodeType,
    ) -> SessionResult<()> {
        self.move_tracker_mut()?.record(from, to, subtree_nodes, node_id, node_type);
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
                my_edit_changes.merge(other_changes)?;
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

    // Sort moves by final path. Path::cmp compares by path components,
    // so children sort between parent and prefix-siblings (e.g. /c < /c/a < /ca).
    let moves: Vec<_> = cs
        .move_tracker()
        .nodes_by_original
        .iter()
        .map(|(from, (to, node_id, node_type))| Move {
            from: from.clone(),
            to: to.clone(),
            node_id: node_id.clone(),
            node_type: node_type.clone(),
        })
        .sorted_by(|a, b| a.to.cmp(&b.to))
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
        moves.into_iter(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use itertools::Itertools as _;

    fn pathify(s: &str) -> Path {
        Path::new(s).unwrap()
    }

    fn record_move(
        mt: &mut MoveTracker,
        from: &str,
        to: &str,
        subtree: &[(&str, NodeId, NodeType)],
        node_id: &NodeId,
        node_type: NodeType,
    ) {
        mt.record(
            pathify(from),
            pathify(to),
            subtree.iter().map(|(path, id, ty)| (pathify(path), id.clone(), ty.clone())),
            node_id,
            node_type,
        );
    }

    use super::{ChangeSet, transaction_log_from_change_set};

    use crate::{
        change_set::{ArrayData, EditChanges, MoveTracker},
        format::{
            ChunkIndices, NodeId, NodeType, Path,
            manifest::{ChunkInfo, ChunkPayload},
            snapshot::ArrayShape,
        },
        session::{SessionError, SessionErrorKind},
    };
    use icechunk_format::roundtrip_serialization_tests;

    fn sample_array_data() -> ArrayData {
        ArrayData {
            shape: ArrayShape::new(vec![(2, 2)]).unwrap(),
            dimension_names: None,
            user_data: Bytes::from_static(b"test"),
        }
    }

    fn sample_array_data_alt() -> ArrayData {
        ArrayData {
            shape: ArrayShape::new(vec![(4, 4)]).unwrap(),
            dimension_names: None,
            user_data: Bytes::from_static(b"other"),
        }
    }

    fn assert_merge_error(result: &Result<(), SessionError>, needle: &str) {
        let err = result.as_ref().unwrap_err();
        match err.kind() {
            SessionErrorKind::SessionMerge(msg) => {
                assert!(msg.contains(needle), "expected '{needle}' in error: {msg}");
            }
            other => panic!("expected SessionMerge, got: {other:?}"),
        }
    }

    #[icechunk_macros::test]
    fn test_merge_conflict_both_created_same_group() {
        let mut a = EditChanges::default();
        let mut b = EditChanges::default();
        let node_id = NodeId::random();
        let p: Path = "/grp".try_into().unwrap();

        a.new_groups.insert(p.clone(), (node_id.clone(), Bytes::from_static(b"v1")));
        b.new_groups.insert(p.clone(), (node_id.clone(), Bytes::from_static(b"v2")));

        let result = a.merge(b.clone());
        assert_merge_error(&result, "Multiple writers created the same groups");

        b.new_groups.insert(p, (node_id, Bytes::from_static(b"v1")));
        assert!(a.merge(b).is_ok());
    }

    #[icechunk_macros::test]
    fn test_merge_conflict_both_updated_same_group() {
        let mut a = EditChanges::default();
        let mut b = EditChanges::default();
        let node_id = NodeId::random();

        a.updated_groups.insert(node_id.clone(), Bytes::from_static(b"v1"));
        b.updated_groups.insert(node_id.clone(), Bytes::from_static(b"v2"));

        let result = a.merge(b.clone());
        assert_merge_error(&result, "Multiple writers updated the same groups");

        b.updated_groups.insert(node_id, Bytes::from_static(b"v1"));
        assert!(a.merge(b).is_ok());
    }

    #[icechunk_macros::test]
    fn test_merge_conflict_both_created_same_array() {
        let mut a = EditChanges::default();
        let mut b = EditChanges::default();
        let node_id = NodeId::random();
        let p: Path = "/arr".try_into().unwrap();

        a.new_arrays.insert(p.clone(), (node_id.clone(), sample_array_data()));
        b.new_arrays.insert(p.clone(), (node_id.clone(), sample_array_data_alt()));

        let result = a.merge(b.clone());
        assert_merge_error(&result, "Multiple writers created the same arrays");

        b.new_arrays.insert(p, (node_id, sample_array_data()));
        assert!(a.merge(b).is_ok());
    }

    #[icechunk_macros::test]
    fn test_merge_conflict_both_updated_same_array() {
        let mut a = EditChanges::default();
        let mut b = EditChanges::default();
        let node_id = NodeId::random();

        a.updated_arrays.insert(node_id.clone(), sample_array_data());
        b.updated_arrays.insert(node_id.clone(), sample_array_data_alt());

        let result = a.merge(b.clone());
        assert_merge_error(&result, "Multiple writers updated the same arrays");

        b.updated_arrays.insert(node_id.clone(), sample_array_data());
        assert!(a.merge(b).is_ok());
    }

    #[icechunk_macros::test]
    fn test_merge_conflict_array_deleted_and_updated() {
        let mut a = EditChanges::default();
        let mut b = EditChanges::default();
        let node_id = NodeId::random();
        let p: Path = "/arr".try_into().unwrap();

        // a deletes the array, b updates it
        a.deleted_arrays.insert((p, node_id.clone()));
        b.updated_arrays.insert(node_id, sample_array_data());

        let result = a.merge(b.clone());
        assert_merge_error(&result, "Arrays were both deleted and modified");

        let result = b.merge(a);
        assert_merge_error(&result, "Arrays were both deleted and modified");
    }

    #[icechunk_macros::test]
    fn test_merge_conflict_array_deleted_and_chunks_set() {
        let mut a = EditChanges::default();
        let mut b = EditChanges::default();
        let node_id = NodeId::random();
        let p: Path = "/arr".try_into().unwrap();

        a.deleted_arrays.insert((p, node_id.clone()));
        b.set_chunks.insert(node_id, Default::default());

        let result = a.merge(b.clone());
        assert_merge_error(&result, "Arrays were both deleted and modified");

        let result = b.merge(a);
        assert_merge_error(&result, "Arrays were both deleted and modified");
    }

    #[icechunk_macros::test]
    fn test_merge_conflict_array_deleted_and_recreated() {
        let mut a = EditChanges::default();
        let mut b = EditChanges::default();
        let node_id = NodeId::random();
        let p: Path = "/arr".try_into().unwrap();

        a.deleted_arrays.insert((p.clone(), node_id));
        b.new_arrays.insert(p, (NodeId::random(), sample_array_data()));

        let result = a.merge(b.clone());
        assert_merge_error(&result, "Arrays were both deleted and modified");

        let result = b.merge(a);
        assert_merge_error(&result, "Arrays were both deleted and modified");
    }

    #[icechunk_macros::test]
    fn test_merge_conflict_array_deleted_and_chunks_outside_bounds() {
        let mut a = EditChanges::default();
        let mut b = EditChanges::default();
        let node_id = NodeId::random();
        let p: Path = "/arr".try_into().unwrap();

        a.deleted_arrays.insert((p, node_id.clone()));
        b.deleted_chunks_outside_bounds.insert(node_id, Default::default());

        let result = a.merge(b.clone());
        assert_merge_error(&result, "Arrays were both deleted and modified");

        let result = b.merge(a);
        assert_merge_error(&result, "Arrays were both deleted and modified");
    }

    #[icechunk_macros::test]
    fn test_merge_conflict_group_deleted_and_updated() {
        let mut a = EditChanges::default();
        let mut b = EditChanges::default();
        let node_id = NodeId::random();
        let p: Path = "/grp".try_into().unwrap();

        a.deleted_groups.insert((p, node_id.clone()));
        b.updated_groups.insert(node_id, Bytes::from_static(b"new"));

        let result = a.merge(b.clone());
        assert_merge_error(&result, "Groups were both deleted and modified");

        let result = b.merge(a);
        assert_merge_error(&result, "Groups were both deleted and modified");
    }

    #[icechunk_macros::test]
    fn test_merge_conflict_group_deleted_and_recreated() {
        let mut a = EditChanges::default();
        let mut b = EditChanges::default();
        let node_id = NodeId::random();
        let p: Path = "/grp".try_into().unwrap();

        a.deleted_groups.insert((p.clone(), node_id));
        b.new_groups.insert(p, (NodeId::random(), Bytes::from_static(b"new")));

        let result = a.merge(b.clone());
        assert_merge_error(&result, "Groups were both deleted and modified");

        let result = b.merge(a);
        assert_merge_error(&result, "Groups were both deleted and modified");
    }

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
        mt.record(
            Path::new("/foo/bar/old").unwrap(),
            Path::new("/foo/bar/new").unwrap(),
            std::iter::empty(),
            &NodeId::random(),
            NodeType::Group,
        );
        mt.record(
            Path::new("/foo/bar/new/inner-old1").unwrap(),
            Path::new("/foo/bar/new/inner-new").unwrap(),
            std::iter::empty(),
            &NodeId::random(),
            NodeType::Group,
        );
        mt.record(
            Path::new("/foo/bar/new/inner-old2").unwrap(),
            Path::new("/inner-new2").unwrap(),
            std::iter::empty(),
            &NodeId::random(),
            NodeType::Group,
        );

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
        mt.record(
            Path::new("/foo/bar/old").unwrap(),
            Path::new("/foo/bar/new").unwrap(),
            std::iter::empty(),
            &NodeId::random(),
            NodeType::Group,
        );
        mt.record(
            Path::new("/foo/bar/new/inner-old1").unwrap(),
            Path::new("/foo/bar/new/inner-new").unwrap(),
            std::iter::empty(),
            &NodeId::random(),
            NodeType::Group,
        );
        mt.record(
            Path::new("/foo/bar/new/inner-old2").unwrap(),
            Path::new("/inner-new2").unwrap(),
            std::iter::empty(),
            &NodeId::random(),
            NodeType::Group,
        );

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
        let node_id = NodeId::random();
        mt.record(
            Path::new("/foo/bar/old").unwrap(),
            Path::new("/foo/bar/new").unwrap(),
            std::iter::empty(),
            &node_id,
            NodeType::Group,
        );
        mt.record(
            Path::new("/foo/bar/new").unwrap(),
            Path::new("/foo/bar/old").unwrap(),
            std::iter::empty(),
            &node_id,
            NodeType::Group,
        );
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
        let node_id = NodeId::random();
        mt.record(
            Path::new("/foo/bar/old").unwrap(),
            Path::new("/foo/bar/new").unwrap(),
            std::iter::empty(),
            &node_id,
            NodeType::Group,
        );
        mt.record(
            Path::new("/foo/bar/new").unwrap(),
            Path::new("/foo/bar/old").unwrap(),
            std::iter::empty(),
            &node_id,
            NodeType::Group,
        );
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
                nodes_by_original: Default::default(),
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

    #[icechunk_macros::test]
    fn test_moved_into() {
        // Tree: /g (group), /g/a (group), /g/a/x (array)
        // Move: /g/a -> /g/b
        // After: /g/b, /g/b/x
        let mut mt = MoveTracker::default();
        record_move(
            &mut mt,
            "/g/a",
            "/g/b",
            &[
                ("/g/a", NodeId::random(), NodeType::Group),
                ("/g/a/x", NodeId::random(), NodeType::Array),
            ],
            &NodeId::random(),
            NodeType::Group,
        );

        assert!(mt.is_remapped(&pathify("/g/a")));
        assert!(mt.is_remapped(&pathify("/g/a/x")));
        assert!(!mt.is_remapped(&pathify("/g/b")));

        let under_g = mt.moved_into(&pathify("/g"));
        assert_eq!(under_g.len(), 2);
        assert!(under_g.contains(&(pathify("/g/a"), pathify("/g/b"))));
        assert!(under_g.contains(&(pathify("/g/a/x"), pathify("/g/b/x"))));
    }

    #[icechunk_macros::test]
    fn test_moved_into_after_move_in_then_rename() {
        // Start:
        // /
        // ├── a         (G)
        // │   └── x     (G)
        // │       └── z [A]
        // └── b         (G)
        //     └── y     [A]
        //
        let grp_a = NodeId::random();
        let grp_ax = NodeId::random();
        let arr_axz = NodeId::random();
        let grp_b = NodeId::random();
        let arr_by = NodeId::random();

        let mut mt = MoveTracker::default();
        // Move 1: /a -> /b/a (move /a into /b)
        // /
        // └── b             (G)
        //     ├── a         (G)
        //     │   └── x     (G)
        //     │       └── z [A]
        //     └── y         [A]
        //
        record_move(
            &mut mt,
            "/a",
            "/b/a",
            &[
                ("/a", grp_a.clone(), NodeType::Group),
                ("/a/x", grp_ax.clone(), NodeType::Group),
                ("/a/x/z", arr_axz.clone(), NodeType::Array),
            ],
            &grp_a,
            NodeType::Group,
        );
        // Move 2: /b -> /c (rename /b — carries along /a, /a/x, /a/x/z)
        // /
        // └── c             (G)
        //     ├── a         (G)
        //     │   └── x     (G)
        //     │       └── z [A]
        //     └── y         [A]
        //
        // subtree_nodes here is missing the nodes moved into /b by move 1.
        // This is not the intended usage, but we protect against it —
        // step 1 of record() handles carried nodes regardless.
        record_move(
            &mut mt,
            "/b",
            "/c",
            &[
                ("/b", grp_b.clone(), NodeType::Group),
                ("/b/y", arr_by.clone(), NodeType::Array),
            ],
            &grp_b,
            NodeType::Group,
        );
        // Move 3: /c/a/x -> /d — subtree_nodes passes current paths,
        // testing that already-tracked nodes are skipped by current path.
        // /
        // ├── c         (G)   # was /b
        // │   ├── a     (G)   # was /a
        // │   └── y     [A]   # was /b/y
        // └── d         (G)   # was /a/x
        //     └── z     [A]   # was /a/x/z
        //
        record_move(
            &mut mt,
            "/c/a/x",
            "/d",
            &[
                ("/c/a/x", grp_ax.clone(), NodeType::Group),
                ("/c/a/x/z", arr_axz.clone(), NodeType::Array),
            ],
            &grp_ax,
            NodeType::Group,
        );
        // Move 4: /d -> /e — subtree_nodes passes original (snapshot) paths.
        // This is not the intended usage, but we protect against it —
        // already-tracked nodes are skipped by original path.
        // /
        // ├── c         (G)   # was /b
        // │   ├── a     (G)   # was /a
        // │   └── y     [A]   # was /b/y
        // └── e         (G)   # was /a/x
        //     └── z     [A]   # was /a/x/z
        //
        record_move(
            &mut mt,
            "/d",
            "/e",
            &[
                ("/a/x", grp_ax.clone(), NodeType::Group),
                ("/a/x/z", arr_axz.clone(), NodeType::Array),
            ],
            &grp_ax,
            NodeType::Group,
        );

        assert_eq!(
            mt.moved_into(&pathify("/c")),
            vec![
                (pathify("/b"), pathify("/c")),
                (pathify("/a"), pathify("/c/a")),
                (pathify("/b/y"), pathify("/c/y")),
            ],
        );
        assert_eq!(
            mt.moved_into(&pathify("/e")),
            vec![(pathify("/a/x"), pathify("/e")), (pathify("/a/x/z"), pathify("/e/z")),],
        );

        // The tx log should contain all moves sorted by final path
        // (parent before child). Build a ChangeSet to test the full path.
        let cs = ChangeSet::Rearrange(mt);
        let snap_id = crate::format::SnapshotId::random();
        let tx = transaction_log_from_change_set(&snap_id, &cs);
        let move_pairs: Vec<_> = tx
            .moves()
            .map(|m| {
                let m = m.expect("invalid move");
                (m.from.clone(), m.to.clone())
            })
            .collect();
        assert_eq!(
            move_pairs,
            vec![
                (pathify("/b"), pathify("/c")),
                (pathify("/a"), pathify("/c/a")),
                (pathify("/b/y"), pathify("/c/y")),
                (pathify("/a/x"), pathify("/e")),
                (pathify("/a/x/z"), pathify("/e/z")),
            ],
        );
    }

    #[icechunk_macros::test]
    fn test_tx_log_moves_sorted_path_aware() {
        // Path sorting must be path-aware: children before non-child siblings.
        // Lexicographic on bytes gives this because '/' (0x2F) < 'a' (0x61),
        // so /c < /c/a < /ca. This test verifies the tx log sort is correct
        // when node names share a prefix.
        //
        // Tree: /ca (group), /ca/x (array), /c (group), /c/a (array)
        // Move: /c -> /d, /ca -> /da
        let mut mt = MoveTracker::default();
        let id_c = NodeId::random();
        let id_ca_arr = NodeId::random();
        let id_ca_grp = NodeId::random();
        let id_cax = NodeId::random();
        record_move(
            &mut mt,
            "/c",
            "/d",
            &[
                ("/c", id_c.clone(), NodeType::Group),
                ("/c/a", id_ca_arr.clone(), NodeType::Array),
            ],
            &id_c,
            NodeType::Group,
        );
        record_move(
            &mut mt,
            "/ca",
            "/da",
            &[
                ("/ca", id_ca_grp.clone(), NodeType::Group),
                ("/ca/x", id_cax.clone(), NodeType::Array),
            ],
            &id_ca_grp,
            NodeType::Group,
        );

        let cs = ChangeSet::Rearrange(mt);
        let snap_id = crate::format::SnapshotId::random();
        let tx = transaction_log_from_change_set(&snap_id, &cs);
        let move_pairs: Vec<_> = tx
            .moves()
            .map(|m| {
                let m = m.expect("invalid move");
                (m.from.clone(), m.to.clone())
            })
            .collect();
        // /d before /d/a before /da (children before prefix-siblings)
        assert_eq!(
            move_pairs,
            vec![
                (pathify("/c"), pathify("/d")),
                (pathify("/c/a"), pathify("/d/a")),
                (pathify("/ca"), pathify("/da")),
                (pathify("/ca/x"), pathify("/da/x")),
            ],
        );
    }

    #[icechunk_macros::test]
    fn test_moved_into_after_move_out_then_rename() {
        // Tree: /a (group), /a/x (array), /a/y (array)
        // Move 1: /a/x -> /b (move x out of a)
        // Move 2: /a -> /c (rename a)
        // After: /b (was /a/x), /c (was /a), /c/y (was /a/y)
        let mut mt = MoveTracker::default();
        record_move(
            &mut mt,
            "/a/x",
            "/b",
            &[("/a/x", NodeId::random(), NodeType::Array)],
            &NodeId::random(),
            NodeType::Array,
        );
        record_move(
            &mut mt,
            "/a",
            "/c",
            &[
                ("/a", NodeId::random(), NodeType::Group),
                ("/a/x", NodeId::random(), NodeType::Array),
                ("/a/y", NodeId::random(), NodeType::Array),
            ],
            &NodeId::random(),
            NodeType::Group,
        );

        // /a/x was moved to /b, not carried along to /c
        let under_c = mt.moved_into(&pathify("/c"));
        assert!(under_c.contains(&(pathify("/a"), pathify("/c"))));
        assert!(under_c.contains(&(pathify("/a/y"), pathify("/c/y"))));
        assert!(!under_c.iter().any(|(_, f)| *f == pathify("/c/x")));

        assert!(
            mt.moved_into(&pathify("/b")).contains(&(pathify("/a/x"), pathify("/b")))
        );
    }
}
