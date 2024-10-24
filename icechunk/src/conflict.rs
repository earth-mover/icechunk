use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    change_set::ChangeSet,
    format::{transaction_log::TransactionLog, NodeId, Path},
    repository::RepositoryResult,
    Repository,
};

#[derive(Debug, PartialEq, Eq)]
pub enum UnsolvableConflict {
    NoFastForwardConfigured,
    ChunksWrittenToDeletedArrays(Vec<Path>),
    WriteToWrittenChunk(HashMap<Path, usize>),
    ConflictingUserAttributesUpdate(Vec<Path>),
    ConflictingZarrMetadataUpdate(Vec<Path>),
    ConflictingGroupCreation(Vec<Path>),
    ConflictingArrayCreation(Vec<Path>),
}

#[derive(Debug)]
pub enum ConflictResolution {
    Patched(ChangeSet),
    Failure { reason: Vec<UnsolvableConflict>, unmodified: ChangeSet },
}

#[async_trait]
pub trait ConflictSolver {
    async fn solve(
        &self,
        previous_change: &TransactionLog,
        previous_repo: &Repository,
        current_changes: ChangeSet,
    ) -> RepositoryResult<ConflictResolution>;
}

pub struct NoFastForward();

#[async_trait]
impl ConflictSolver for NoFastForward {
    async fn solve(
        &self,
        _previous_change: &TransactionLog,
        _previous_repo: &Repository,
        current_changes: ChangeSet,
    ) -> RepositoryResult<ConflictResolution> {
        Ok(ConflictResolution::Failure {
            reason: vec![UnsolvableConflict::NoFastForwardConfigured],
            unmodified: current_changes,
        })
    }
}

pub enum VersionSelection {
    Fail,
    Ours,
    Theirs,
}

pub struct BasicConflictSolver {
    pub on_chunk_write_conflicts: VersionSelection,
    pub on_user_attributes_conflict: VersionSelection,
    pub on_zarr_metadata_conflict: VersionSelection,
    pub on_group_creation_conflict: VersionSelection,
    pub on_array_creation_conflict: VersionSelection,
}

impl Default for BasicConflictSolver {
    fn default() -> Self {
        Self {
            on_chunk_write_conflicts: VersionSelection::Fail,
            on_user_attributes_conflict: VersionSelection::Fail,
            on_zarr_metadata_conflict: VersionSelection::Fail,
            on_group_creation_conflict: VersionSelection::Fail,
            on_array_creation_conflict: VersionSelection::Fail,
        }
    }
}

#[async_trait]
impl ConflictSolver for BasicConflictSolver {
    async fn solve(
        &self,
        previous_change: &TransactionLog,
        previous_repo: &Repository,
        mut current_changes: ChangeSet,
    ) -> RepositoryResult<ConflictResolution> {
        let write_chunk_to_deleted_array_conflicts = current_changes
            .written_arrays()
            .filter_map(|node_id| previous_change.deleted_arrays.get(node_id))
            .cloned()
            .collect::<Vec<_>>();

        let mut write_to_written_chunks_conflicts = HashMap::new();
        for (node, indices_written) in current_changes.all_modified_chunks_iterator() {
            if let Some(previous_updates) = previous_change.updated_chunks.get(node) {
                let conflicts = indices_written
                    .filter(|idx| previous_updates.contains(idx))
                    .cloned()
                    .collect::<Vec<_>>();
                if !conflicts.is_empty() {
                    let path = find_path(node, previous_repo)
                        .await?
                        .expect("Bug in conflict detection");
                    write_to_written_chunks_conflicts.insert((*node, path), conflicts);
                }
            }
        }

        let mut both_updated_attributes_conflicts = Vec::new();
        for node in previous_change
            .updated_user_attributes
            .iter()
            .filter(|node| current_changes.has_updated_attributes(node))
        {
            let path =
                find_path(node, previous_repo).await?.expect("Bug in conflict detection");
            both_updated_attributes_conflicts.push(path);
        }

        let mut both_updated_zarr_metadata_conflicts = Vec::new();
        for node in previous_change
            .updated_zarr_metadata
            .iter()
            .filter(|node| current_changes.get_updated_zarr_metadata(**node).is_some())
        {
            let path =
                find_path(node, previous_repo).await?.expect("Bug in conflict detection");
            both_updated_zarr_metadata_conflicts.push(path);
        }

        let both_created_group_conflicts = current_changes
            .new_groups()
            .filter(|path| previous_change.new_groups.contains_key(path))
            .cloned()
            .collect::<Vec<_>>();

        let both_created_array_conflicts = current_changes
            .new_arrays()
            .filter(|path| previous_change.new_arrays.contains_key(path))
            .cloned()
            .collect::<Vec<_>>();

        let mut conflicts = Vec::new();

        if !write_chunk_to_deleted_array_conflicts.is_empty() {
            conflicts.push(UnsolvableConflict::ChunksWrittenToDeletedArrays(
                write_chunk_to_deleted_array_conflicts,
            ));
        }

        let mut delete_our_conflict_chunks = None;

        if !write_to_written_chunks_conflicts.is_empty() {
            match self.on_chunk_write_conflicts {
                VersionSelection::Fail => {
                    conflicts.push(UnsolvableConflict::WriteToWrittenChunk(
                        write_to_written_chunks_conflicts
                            .into_iter()
                            .map(|((_node, path), conflicts)| (path, conflicts.len()))
                            .collect(),
                    ));
                }
                VersionSelection::Ours => {
                    // Nothing to do, our chunks will win
                }
                VersionSelection::Theirs => {
                    delete_our_conflict_chunks = Some(write_to_written_chunks_conflicts);
                }
            }
        }

        // let mut delete_our_user_attributes = false;

        // if !both_updated_attributes_conflicts.is_empty() {
        //     match self.on_user_attributes_conflict {
        //         VersionSelection::Fail => {
        //             conflicts.push(UnsolvableConflict::ConflictingUserAttributesUpdate(
        //                 both_updated_attributes_conflicts,
        //             ));
        //         }
        //         VersionSelection::Ours => {
        //             // Nothing to do, our metadata will win
        //         }
        //         VersionSelection::Theirs => {
        //             delete_our_user_attributes = true;
        //         }
        //     }
        // }

        // let mut delete_our_updated_zarr_metadata = false;

        // if !both_updated_zarr_metadata_conflicts.is_empty() {
        //     match self.on_zarr_metadata_conflict {
        //         VersionSelection::Fail => {
        //             conflicts.push(UnsolvableConflict::ConflictingZarrMetadataUpdate(
        //                 both_updated_zarr_metadata_conflicts,
        //             ));
        //         }
        //         VersionSelection::Ours => {
        //             // Nothing to do, our user atts will win
        //         }
        //         VersionSelection::Theirs => {
        //             delete_our_updated_zarr_metadata = true;
        //         }
        //     }
        // }

        // let mut delete_our_new_groups = false;

        // if !both_created_group_conflicts.is_empty() {
        //     match self.on_group_creation_conflict {
        //         VersionSelection::Fail => {
        //             conflicts.push(UnsolvableConflict::ConflictingGroupCreation(
        //                 both_created_group_conflicts,
        //             ));
        //         }
        //         VersionSelection::Ours => {
        //             // Nothing to do, our groups will win
        //         }
        //         VersionSelection::Theirs => {
        //             delete_our_new_groups = true;
        //         }
        //     }
        // }

        // let mut delete_our_new_arrays = false;

        // if !both_created_array_conflicts.is_empty() {
        //     match self.on_array_creation_conflict {
        //         VersionSelection::Fail => {
        //             conflicts.push(UnsolvableConflict::ConflictingArrayCreation(
        //                 both_created_array_conflicts,
        //             ));
        //         }
        //         VersionSelection::Ours => {
        //             // Nothing to do, our groups will win
        //         }
        //         VersionSelection::Theirs => {
        //             delete_our_new_arrays = true;
        //         }
        //     }
        // }

        if conflicts.is_empty() {
            //fixme
            if let Some(chunks) = delete_our_conflict_chunks {
                for ((node, path), conflicting_indices) in chunks {
                    for idx in conflicting_indices.iter() {
                        current_changes.unset_chunk_ref(node, idx);
                    }
                }
            }

            Ok(ConflictResolution::Patched(current_changes))
        } else {
            Ok(ConflictResolution::Failure {
                reason: conflicts,
                unmodified: current_changes,
            })
        }
    }
}

async fn find_path(node: &NodeId, repo: &Repository) -> RepositoryResult<Option<Path>> {
    Ok(repo.list_nodes().await?.find_map(|node_snap| {
        if node_snap.id == *node {
            Some(node_snap.path)
        } else {
            None
        }
    }))
}
