use async_trait::async_trait;

use crate::{
    change_set::ChangeSet, format::transaction_log::TransactionLog,
    repository::RepositoryResult, Repository,
};

use super::{detector::ConflictDetector, Conflict, ConflictResolution, ConflictSolver};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionSelection {
    Fail,
    UseOurs,
    UseTheirs,
}

#[derive(Debug, Clone)]
pub struct BasicConflictSolver {
    pub on_user_attributes_conflict: VersionSelection,
    pub on_chunk_conflict: VersionSelection,
    pub fail_on_delete_of_updated_array: bool,
    pub fail_on_delete_of_updated_group: bool,
    pub fail_on_delete_of_updated_descendants: bool,
}

impl Default for BasicConflictSolver {
    fn default() -> Self {
        Self {
            on_user_attributes_conflict: VersionSelection::UseOurs,
            on_chunk_conflict: VersionSelection::UseOurs,
            fail_on_delete_of_updated_array: false,
            fail_on_delete_of_updated_group: false,
            fail_on_delete_of_updated_descendants: false,
        }
    }
}

#[async_trait]
impl ConflictSolver for BasicConflictSolver {
    async fn solve(
        &self,
        previous_change: &TransactionLog,
        previous_repo: &Repository,
        current_changes: ChangeSet,
        current_repo: &Repository,
    ) -> RepositoryResult<ConflictResolution> {
        match ConflictDetector
            .solve(previous_change, previous_repo, current_changes, current_repo)
            .await?
        {
            res @ ConflictResolution::Patched(_) => Ok(res),
            ConflictResolution::Unsolvable { reason, unmodified } => {
                self.solve_conflicts(
                    previous_change,
                    previous_repo,
                    unmodified,
                    current_repo,
                    reason,
                )
                .await
            }
        }
    }
}

impl BasicConflictSolver {
    async fn solve_conflicts(
        &self,
        _previous_change: &TransactionLog,
        _previous_repo: &Repository,
        current_changes: ChangeSet,
        _current_repo: &Repository,
        conflicts: Vec<Conflict>,
    ) -> RepositoryResult<ConflictResolution> {
        use Conflict::*;
        let unsolvable = conflicts.iter().any(
            |conflict| {
                matches!(
                    conflict,
                    NewNodeConflictsWithExistingNode(_) |
                    NewNodeInInvalidGroup(_) |
                    ZarrMetadataDoubleUpdate(_) |
                    ZarrMetadataUpdateOfDeletedArray(_) |
                    UserAttributesUpdateOfDeletedNode(_) |
                    ChunksUpdatedInDeletedArray{..} |
                    ChunksUpdatedInUpdatedArray{..}
                ) ||
                matches!(conflict,
                    UserAttributesDoubleUpdate{..} if self.on_user_attributes_conflict == VersionSelection::Fail
                ) ||
                matches!(conflict,
                    ChunkDoubleUpdate{..} if self.on_chunk_conflict == VersionSelection::Fail
                ) ||
                matches!(conflict,
                    DeleteOfUpdatedArray(_) if self.fail_on_delete_of_updated_array
                ) ||
                matches!(conflict,
                    DeleteOfUpdatedGroup(_) if self.fail_on_delete_of_updated_group
                ) ||
                matches!(conflict,
                    DeleteOfUpdatedGroupDescendants(_) if self.fail_on_delete_of_updated_descendants
                )
            },
        );

        if unsolvable {
            return Ok(ConflictResolution::Unsolvable {
                reason: conflicts,
                unmodified: current_changes,
            });
        }

        let mut current_changes = current_changes;
        for conflict in conflicts {
            match conflict {
                ChunkDoubleUpdate { node_id, chunk_coordinates, .. } => {
                    match self.on_chunk_conflict {
                        VersionSelection::UseOurs => {
                            // this is a no-op, our change will override the conflicting change
                        }
                        VersionSelection::UseTheirs => {
                            let changes = current_changes.set_chunks.get_mut(&node_id).expect("Bug in conflict resolution: node_id not found in set_chunks map");
                            changes.retain(|coord,_| ! chunk_coordinates.contains(coord));
                        }
                        VersionSelection::Fail => panic!("Bug in conflict resolution: ChunkDoubleUpdate flagged as unrecoverable")
                    }
                }
                UserAttributesDoubleUpdate { node_id, .. } => {
                    match self.on_user_attributes_conflict {
                        VersionSelection::UseOurs => {
                            // this is a no-op, our change will override the conflicting change
                        }
                        VersionSelection::UseTheirs => {
                            current_changes.updated_attributes.remove(&node_id);
                        }
                        VersionSelection::Fail => panic!("Bug in conflict resolution: UserAttributesDoubleUpdate flagged as unrecoverable")
                    }
                }
                DeleteOfUpdatedArray(_) => {
                    assert!(!self.fail_on_delete_of_updated_array);
                    // this is a no-op, the solution is to still delete the array
                }
                DeleteOfUpdatedGroup(_) => {
                    assert!(!self.fail_on_delete_of_updated_group);
                    // this is a no-op, the solution is to still delete the group
                }
                DeleteOfUpdatedGroupDescendants(_) => {
                    assert!(!self.fail_on_delete_of_updated_descendants);
                    // this is a no-op, the solution is to still delete the group
                }
                _ => panic!("bug in conflict resolution, conflict: {:?}", conflict),
            }
        }

        Ok(ConflictResolution::Patched(current_changes))
    }
}
