use std::collections::HashSet;

use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};

use crate::{
    change_set::ChangeSet,
    format::transaction_log::TransactionLog,
    repository::{RepositoryError, RepositoryResult},
    Repository,
};

use super::{find_path, Conflict, ConflictResolution, ConflictSolver};

pub struct ConflictDetector;

#[async_trait]
impl ConflictSolver for ConflictDetector {
    async fn solve(
        &self,
        previous_change: &TransactionLog,
        previous_repo: &Repository,
        current_changes: ChangeSet,
        current_repo: &Repository,
    ) -> RepositoryResult<ConflictResolution> {
        let new_nodes_explicit_conflicts = stream::iter(
            current_changes.new_nodes().map(Ok),
        )
        .try_filter_map(|path| async {
            match previous_repo.get_node(path).await {
                Ok(_) => {
                    Ok(Some(Conflict::NewNodeConflictsWithExistingNode(path.clone())))
                }
                Err(RepositoryError::NodeNotFound { .. }) => Ok(None),
                Err(err) => Err(err),
            }
        });

        let new_nodes_implicit_conflicts = stream::iter(
            current_changes.new_nodes().map(Ok),
        )
        .try_filter_map(|path| async {
            for parent in path.ancestors().skip(1) {
                match previous_repo.get_array(&parent).await {
                    Ok(_) => return Ok(Some(Conflict::NewNodeInInvalidGroup(parent))),
                    Err(RepositoryError::NodeNotFound { .. }) => {}
                    Err(err) => return Err(err),
                }
            }
            Ok(None)
        });

        let updated_arrays_already_updated = current_changes
            .updated_arrays
            .keys()
            .filter(|node_id| previous_change.updated_zarr_metadata.contains(node_id))
            .map(Ok);

        let updated_arrays_already_updated = stream::iter(updated_arrays_already_updated)
            .and_then(|node_id| async {
                let path = find_path(node_id, current_repo)
                    .await?
                    .expect("Bug in conflict detection");
                Ok(Conflict::ZarrMetadataDoubleUpdate(path))
            });

        let updated_arrays_were_deleted = current_changes
            .updated_arrays
            .keys()
            .filter(|node_id| previous_change.deleted_arrays.contains(node_id))
            .map(Ok);

        let updated_arrays_were_deleted = stream::iter(updated_arrays_were_deleted)
            .and_then(|node_id| async {
                let path = find_path(node_id, current_repo)
                    .await?
                    .expect("Bug in conflict detection");
                Ok(Conflict::ZarrMetadataUpdateOfDeletedArray(path))
            });

        let updated_attributes_already_updated = current_changes
            .updated_attributes
            .keys()
            .filter(|node_id| previous_change.updated_user_attributes.contains(node_id))
            .map(Ok);

        let updated_attributes_already_updated =
            stream::iter(updated_attributes_already_updated).and_then(|node_id| async {
                let path = find_path(node_id, current_repo)
                    .await?
                    .expect("Bug in conflict detection");
                Ok(Conflict::UserAttributesDoubleUpdate {
                    path,
                    node_id: node_id.clone(),
                })
            });

        let updated_attributes_on_deleted_node = current_changes
            .updated_arrays
            .keys()
            .filter(|node_id| {
                previous_change.deleted_arrays.contains(node_id)
                    || previous_change.deleted_groups.contains(node_id)
            })
            .map(Ok);

        let updated_attributes_on_deleted_node =
            stream::iter(updated_attributes_on_deleted_node).and_then(|node_id| async {
                let path = find_path(node_id, current_repo)
                    .await?
                    .expect("Bug in conflict detection");
                Ok(Conflict::UserAttributesUpdateOfDeletedNode(path))
            });

        let chunks_updated_in_deleted_array = current_changes
            .set_chunks
            .keys()
            .filter(|node_id| previous_change.deleted_arrays.contains(node_id))
            .map(Ok);

        let chunks_updated_in_deleted_array =
            stream::iter(chunks_updated_in_deleted_array).and_then(|node_id| async {
                let path = find_path(node_id, current_repo)
                    .await?
                    .expect("Bug in conflict detection");
                Ok(Conflict::ChunksUpdatedInDeletedArray {
                    path,
                    node_id: node_id.clone(),
                })
            });

        let chunks_updated_in_updated_array = current_changes
            .set_chunks
            .keys()
            .filter(|node_id| previous_change.updated_zarr_metadata.contains(node_id))
            .map(Ok);

        let chunks_updated_in_updated_array =
            stream::iter(chunks_updated_in_updated_array).and_then(|node_id| async {
                let path = find_path(node_id, current_repo)
                    .await?
                    .expect("Bug in conflict detection");
                Ok(Conflict::ChunksUpdatedInUpdatedArray {
                    path,
                    node_id: node_id.clone(),
                })
            });

        let chunks_double_updated =
            current_changes.set_chunks.iter().filter_map(|(node_id, changes)| {
                if let Some(previous_changes) =
                    previous_change.updated_chunks.get(node_id)
                {
                    let conflicting: HashSet<_> = changes
                        .keys()
                        .filter(|coord| previous_changes.contains(coord))
                        .cloned()
                        .collect();
                    if conflicting.is_empty() {
                        None
                    } else {
                        Some(Ok((node_id, conflicting)))
                    }
                } else {
                    None
                }
            });

        let chunks_double_updated = stream::iter(chunks_double_updated).and_then(
            |(node_id, conflicting_coords)| async {
                let path = find_path(node_id, current_repo)
                    .await?
                    .expect("Bug in conflict detection");
                Ok(Conflict::ChunkDoubleUpdate {
                    path,
                    node_id: node_id.clone(),
                    chunk_coordinates: conflicting_coords,
                })
            },
        );

        let all_conflicts: Vec<_> = new_nodes_explicit_conflicts
            .chain(new_nodes_implicit_conflicts)
            .chain(updated_arrays_already_updated)
            .chain(updated_arrays_were_deleted)
            .chain(updated_attributes_already_updated)
            .chain(updated_attributes_on_deleted_node)
            .chain(chunks_updated_in_deleted_array)
            .chain(chunks_updated_in_updated_array)
            .chain(chunks_double_updated)
            .try_collect()
            .await?;

        if all_conflicts.is_empty() {
            Ok(ConflictResolution::Patched(current_changes))
        } else {
            Ok(ConflictResolution::Unsolvable {
                reason: all_conflicts,
                unmodified: current_changes,
            })
        }
    }
}
