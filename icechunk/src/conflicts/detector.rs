use std::{
    collections::{HashMap, HashSet},
    ops::DerefMut,
    sync::Mutex,
};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream};

use crate::{
    change_set::ChangeSet,
    format::{NodeId, Path, snapshot::NodeSnapshot, transaction_log::TransactionLog},
    session::{Session, SessionError, SessionErrorKind, SessionResult},
};

use super::{Conflict, ConflictResolution, ConflictSolver};

#[derive(Debug, Clone)]
pub struct ConflictDetector;

#[async_trait]
impl ConflictSolver for ConflictDetector {
    async fn solve(
        &self,
        previous_change: &TransactionLog,
        previous_repo: &Session,
        current_changes: ChangeSet,
        current_repo: &Session,
    ) -> SessionResult<ConflictResolution> {
        let new_nodes_explicit_conflicts = stream::iter(
            current_changes.new_nodes().map(Ok),
        )
        .try_filter_map(|(path, _)| async {
            match previous_repo.get_node(path).await {
                Ok(_) => {
                    Ok(Some(Conflict::NewNodeConflictsWithExistingNode(path.clone())))
                }
                Err(SessionError {
                    kind: SessionErrorKind::NodeNotFound { .. }, ..
                }) => Ok(None),
                Err(err) => Err(err),
            }
        });

        let new_nodes_implicit_conflicts = stream::iter(
            current_changes.new_nodes().map(Ok),
        )
        .try_filter_map(|(path, _)| async {
            for parent in path.ancestors().skip(1) {
                match previous_repo.get_array(&parent).await {
                    Ok(_) => return Ok(Some(Conflict::NewNodeInInvalidGroup(parent))),
                    Err(SessionError {
                        kind: SessionErrorKind::NodeNotFound { .. },
                        ..
                    })
                    | Err(SessionError {
                        kind: SessionErrorKind::NotAnArray { .. },
                        ..
                    }) => {}
                    Err(err) => return Err(err),
                }
            }
            Ok(None)
        });

        let path_finder = PathFinder::new(current_repo.list_nodes().await?);

        let updated_arrays_already_updated = current_changes
            .updated_arrays()
            .filter(|node_id| previous_change.array_updated(node_id))
            .map(Ok);

        let updated_arrays_already_updated = stream::iter(updated_arrays_already_updated)
            .and_then(|node_id| async {
                let path = path_finder.find(node_id)?;
                Ok(Conflict::ZarrMetadataDoubleUpdate(path))
            });

        let updated_groups_already_updated = current_changes
            .updated_groups()
            .filter(|node_id| previous_change.group_updated(node_id))
            .map(Ok);

        let updated_groups_already_updated = stream::iter(updated_groups_already_updated)
            .and_then(|node_id| async {
                let path = path_finder.find(node_id)?;
                Ok(Conflict::ZarrMetadataDoubleUpdate(path))
            });

        let updated_arrays_were_deleted = current_changes
            .updated_arrays()
            .filter(|node_id| previous_change.array_deleted(node_id))
            .map(Ok);

        let updated_arrays_were_deleted = stream::iter(updated_arrays_were_deleted)
            .and_then(|node_id| async {
                let path = path_finder.find(node_id)?;
                Ok(Conflict::ZarrMetadataUpdateOfDeletedArray(path))
            });

        let updated_groups_were_deleted = current_changes
            .updated_groups()
            .filter(|node_id| previous_change.group_deleted(node_id))
            .map(Ok);

        let updated_groups_were_deleted = stream::iter(updated_groups_were_deleted)
            .and_then(|node_id| async {
                let path = path_finder.find(node_id)?;
                Ok(Conflict::ZarrMetadataUpdateOfDeletedGroup(path))
            });

        let chunks_updated_in_deleted_array = current_changes
            .arrays_with_chunk_changes()
            .filter(|node_id| previous_change.array_deleted(node_id))
            .map(Ok);

        let chunks_updated_in_deleted_array =
            stream::iter(chunks_updated_in_deleted_array).and_then(|node_id| async {
                let path = path_finder.find(node_id)?;
                Ok(Conflict::ChunksUpdatedInDeletedArray {
                    path,
                    node_id: node_id.clone(),
                })
            });

        let chunks_updated_in_updated_array = current_changes
            .arrays_with_chunk_changes()
            .filter(|node_id| previous_change.array_updated(node_id))
            .map(Ok);

        let chunks_updated_in_updated_array =
            stream::iter(chunks_updated_in_updated_array).and_then(|node_id| async {
                let path = path_finder.find(node_id)?;
                Ok(Conflict::ChunksUpdatedInUpdatedArray {
                    path,
                    node_id: node_id.clone(),
                })
            });

        let chunks_double_updated =
            current_changes.changed_chunks().filter_map(|(node_id, changes)| {
                let previous_changes: HashSet<_> =
                    previous_change.updated_chunks_for(node_id).collect();
                if previous_changes.is_empty() {
                    None
                } else {
                    let conflicting: HashSet<_> = changes
                        .filter(|coord| previous_changes.contains(coord))
                        .cloned()
                        .collect();
                    if conflicting.is_empty() {
                        None
                    } else {
                        Some(Ok((node_id, conflicting)))
                    }
                }
            });

        let chunks_double_updated = stream::iter(chunks_double_updated).and_then(
            |(node_id, conflicting_coords)| async {
                let path = path_finder.find(node_id)?;
                Ok(Conflict::ChunkDoubleUpdate {
                    path,
                    node_id: node_id.clone(),
                    chunk_coordinates: conflicting_coords,
                })
            },
        );

        let deletes_of_updated_arrays = stream::iter(
            current_changes.deleted_arrays().map(Ok),
        )
        .try_filter_map(|(path, _node_id)| async {
            let id = match previous_repo.get_node(path).await {
                Ok(node) => Some(node.id),
                Err(SessionError {
                    kind: SessionErrorKind::NodeNotFound { .. }, ..
                }) => None,
                Err(err) => Err(err)?,
            };

            if let Some(node_id) = id {
                if previous_change.array_updated(&node_id)
                    || previous_change.chunks_updated(&node_id)
                {
                    Ok(Some(Conflict::DeleteOfUpdatedArray {
                        path: path.clone(),
                        node_id: node_id.clone(),
                    }))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        });

        let deletes_of_updated_groups = stream::iter(
            current_changes.deleted_groups().map(Ok),
        )
        .try_filter_map(|(path, _node_id)| async {
            let id = match previous_repo.get_node(path).await {
                Ok(node) => Some(node.id),
                Err(SessionError {
                    kind: SessionErrorKind::NodeNotFound { .. }, ..
                }) => None,
                Err(err) => Err(err)?,
            };

            if let Some(node_id) = id {
                if previous_change.group_updated(&node_id) {
                    Ok(Some(Conflict::DeleteOfUpdatedGroup {
                        path: path.clone(),
                        node_id: node_id.clone(),
                    }))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        });

        let all_conflicts: Vec<_> = new_nodes_explicit_conflicts
            .chain(new_nodes_implicit_conflicts)
            .chain(updated_arrays_already_updated)
            .chain(updated_groups_already_updated)
            .chain(updated_arrays_were_deleted)
            .chain(updated_groups_were_deleted)
            .chain(chunks_updated_in_deleted_array)
            .chain(chunks_updated_in_updated_array)
            .chain(chunks_double_updated)
            .chain(deletes_of_updated_arrays)
            .chain(deletes_of_updated_groups)
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

struct PathFinder<It>(Mutex<(HashMap<NodeId, Path>, Option<It>)>);

impl<It: Iterator<Item = SessionResult<NodeSnapshot>>> PathFinder<It> {
    fn new(iter: It) -> Self {
        Self(Mutex::new((HashMap::new(), Some(iter))))
    }

    fn find(&self, node_id: &NodeId) -> SessionResult<Path> {
        // we can safely unwrap the result of `lock` because there is no failing code called while
        // the mutex is hold. The mutex is there purely to support interior mutability
        #![allow(clippy::expect_used)]
        let mut guard = self.0.lock().expect("Concurrency bug in PathFinder");

        let (cache, iter) = guard.deref_mut();
        if let Some(cached) = cache.get(node_id) {
            Ok(cached.clone())
        } else if let Some(iterator) = iter {
            for node in iterator {
                let node = node?;
                if &node.id == node_id {
                    cache.insert(node.id, node.path.clone());
                    return Ok(node.path);
                } else {
                    cache.insert(node.id, node.path);
                }
            }
            *iter = None;
            Err(SessionErrorKind::ConflictingPathNotFound(node_id.clone()).into())
        } else {
            Err(SessionErrorKind::ConflictingPathNotFound(node_id.clone()).into())
        }
    }
}
