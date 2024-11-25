use std::collections::HashSet;

use async_trait::async_trait;

use crate::{
    change_set::ChangeSet,
    format::{transaction_log::TransactionLog, ChunkIndices, NodeId, Path},
    repository::RepositoryResult,
    Repository,
};

pub mod basic_solver;
pub mod detector;

#[derive(Debug, PartialEq, Eq)]
pub enum Conflict {
    NewNodeConflictsWithExistingNode(Path),
    NewNodeInInvalidGroup(Path),
    ZarrMetadataDoubleUpdate(Path),
    ZarrMetadataUpdateOfDeletedArray(Path),
    UserAttributesDoubleUpdate {
        path: Path,
        node_id: NodeId,
    },
    UserAttributesUpdateOfDeletedNode(Path),
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
    DeleteOfUpdatedArray(Path),
    DeleteOfUpdatedGroup(Path),
    // FIXME: we are missing the case of current change deleting a group and previous change
    // creating something new under it
}

#[derive(Debug)]
pub enum ConflictResolution {
    Patched(ChangeSet),
    Unsolvable { reason: Vec<Conflict>, unmodified: ChangeSet },
}

#[async_trait]
pub trait ConflictSolver {
    async fn solve(
        &self,
        previous_change: &TransactionLog,
        previous_repo: &Repository,
        current_changes: ChangeSet,
        current_repo: &Repository,
    ) -> RepositoryResult<ConflictResolution>;
}
