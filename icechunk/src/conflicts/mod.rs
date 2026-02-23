//! Detection and resolution for concurrent writes.
//!
//! When rebasing a session onto a newer snapshot, conflicts may occur if both
//! modified the same data. This module detects conflicts and provides resolution
//! strategies.

use std::collections::HashSet;

use async_trait::async_trait;

use crate::{
    change_set::ChangeSet,
    format::{ChunkIndices, NodeId, Path, transaction_log::TransactionLog},
    session::{Session, SessionResult},
};

pub mod basic_solver;
pub mod detector;

/// A conflict detected when rebasing changes onto a newer snapshot.
#[derive(Debug, PartialEq, Eq)]
pub enum Conflict {
    NewNodeConflictsWithExistingNode(Path),
    NewNodeInInvalidGroup(Path),
    ZarrMetadataDoubleUpdate(Path),
    ZarrMetadataUpdateOfDeletedArray(Path),
    ZarrMetadataUpdateOfDeletedGroup(Path),
    ChunkDoubleUpdate {
        path: Path,
        node_id: NodeId,
        chunk_coordinates: HashSet<ChunkIndices>,
    },
    ChunksUpdatedInDeletedArray {
        path: Path,
        node_id: NodeId,
    },
    ChunksUpdatedInUpdatedArray {
        path: Path,
        node_id: NodeId,
    },
    DeleteOfUpdatedArray {
        path: Path,
        node_id: NodeId,
    },
    DeleteOfUpdatedGroup {
        path: Path,
        node_id: NodeId,
    },
    MoveOperationCannotBeRebased,
    // FIXME: we are missing the case of current change deleting a group and previous change
    // creating something new under it
}

/// Result of attempting to resolve conflicts.
#[derive(Debug)]
pub enum ConflictResolution {
    /// Conflicts were resolved; use this patched changeset.
    Patched(ChangeSet),
    /// Conflicts could not be resolved automatically.
    Unsolvable { reason: Vec<Conflict>, unmodified: ChangeSet },
}

/// Strategy for resolving conflicts during rebase.
#[async_trait]
pub trait ConflictSolver {
    async fn solve(
        &self,
        previous_change: &TransactionLog,
        previous_repo: &Session,
        current_changes: ChangeSet,
        current_repo: &Session,
    ) -> SessionResult<ConflictResolution>;
}
